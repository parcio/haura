use crate::storage_pool::NUM_STORAGE_CLASSES;
use std::collections::HashMap;

use super::ObjectKey;
// This file contains a migration policy based on reinforcement learning.
// We based our approach on the description of
// https://doi.org/10.1109/TKDE.2022.3176753 and aim to use them for object
// migration.
//
// # Description
// The approach is hotness based, meaning we have to calculate a hotness for each file.
//
// ## Input
//
// - s_1 = average temperature of all files in one tier
// - s_2 = average weigthed temperature (temperature x file size)
// - s_3 = queueing times (time until a request can be handle or is completed?), indicate latency
//

mod learning {
    //! This module contains code to encapsulate the training procedure from the
    //! reinforcement learning apprach by Zhang et al.

    use std::{collections::HashMap, ops::Index, time::Duration};

    use crate::{migration::ObjectKey, StoragePreference};

    const EULER: f32 = 2.71828;

    // How often a file is accessed in certain time range.
    pub struct Hotness(f32);

    pub struct Tier {
        level: StoragePreference,
        files: HashMap<ObjectKey, (Hotness, u64)>,
        reqs: HashMap<ObjectKey, Vec<Request>>,
    }

    /// The state of a single tier.
    struct State(Hotness, Hotness, Duration);

    impl State {
        fn membership(&self, agent: &TDAgent) -> [Categories; 3] {
            let large_0 = 1.0 / (1.0 + agent.a_i[0] * EULER.powf(-agent.b_i[0] * self.0 .0));
            let large_1 = 1.0 / (1.0 + agent.a_i[1] * EULER.powf(-agent.b_i[1] * self.1 .0));
            let large_2 =
                1.0 / (1.0 + agent.a_i[2] * EULER.powf(-agent.b_i[2] * self.2.as_secs_f32()));
            [
                Categories {
                    large: large_0,
                    small: 1.0 - large_0,
                },
                Categories {
                    large: large_1,
                    small: 1.0 - large_1,
                },
                Categories {
                    large: large_2,
                    small: 1.0 - large_2,
                },
            ]
        }
    }

    /// Fuzzy Rule Based Function to use the floating input to certain Categories
    struct Categories {
        small: f32,
        large: f32,
    }

    impl Index<usize> for Categories {
        type Output = f32;

        fn index(&self, index: usize) -> &Self::Output {
            match index {
                0 => &self.small,
                1 => &self.large,
                _ => panic!("Category index out of bounds"),
            }
        }
    }

    // Translated from https://github.com/JSFRi/HSM-RL
    struct TDAgent {
        p: [f32; 8],
        z: [f32; 8],
        a_i: [f32; 3],
        b_i: [f32; 3],
        alpha: [f32; 8],
        beta: f32,
        lambda: f32,
    }

    enum Action {
        Promote(),
        Demote(),
    }

    struct Request {
        key: ObjectKey,
        response_time: Duration,
    }

    impl TDAgent {
        fn build(p_init: [f32; 8], beta: f32, lambda: f32, a_i: [f32; 3], b_i: [f32; 3]) -> Self {
            Self {
                alpha: [0.0; 8],
                z: [0.0; 8],
                p: p_init,
                a_i,
                b_i,
                beta,
                lambda,
            }
        }

        fn learn(
            &mut self,
            state: State,
            reward: f32,
            state_next: State,
            phi_list: Vec<[f32; 8]>,
        ) -> [f32; 8] {
            let (c_n, phi_n) = self.cost_phy(state);
            let (c_n_1, phi) = self.cost_phy(state_next);

            for idx in 0..self.alpha.len() {
                self.alpha[idx] = 0.1 / (1.0 + 100.0 * phi_list[idx][..7].iter().sum())
            }

            for idx in 0..self.z.len() {
                self.z[idx] =
                    self.lambda * EULER.powf(-self.beta * state.2.as_secs_f32()) * self.z[idx]
                        + phi_n[idx];
            }

            for idx in 0..self.p.len() {
                self.p[idx] = self.p[idx]
                    + self.alpha[idx]
                        * (reward + EULER.powf(-self.beta * state.2.as_secs_f32()) * c_n_1 - c_n)
                        * self.z[idx];
            }

            return phi;
        }

        fn cost_phy(&self, state: State) -> (f32, [f32; 8]) {
            let membership = state.membership(&self);
            let mut w = [0.0; 8];
            let mut num = 0;
            for x in 0..2 {
                for y in 0..2 {
                    for z in 0..2 {
                        w[num] = membership[0][x] * membership[1][y] * membership[2][z];
                        num += 1;
                    }
                }
            }
            let w_sum: f32 = w.iter().sum();
            // keep phi stack local
            let mut phi = [0.0; 8];
            for idx in 0..phi.len() {
                phi[idx] = w[idx] / w_sum;
            }
            let mut c = 0.0;
            for idx in 0..phi.len() {
                c += self.p[idx] * phi[idx];
            }
            return (c, phi);
        }

        // NOTE: reqs contains requests for this specific tier.
        fn c_up_c_not(
            &self,
            tier: Tier,
            obj: ObjectKey,
            temp: (Hotness, u64),
            obj_reqs: Vec<Request>,
        ) -> (f32, Hotness, f32, Hotness) {
            // This method branches into two main flows in the original one handling if the file is contained in the tier and one if it is not
            let s1_not;
            let s2_not;
            let s3_not;

            let num_reqs;
            if tier.files.is_empty() {
                s1_not = Hotness(0.0);
                s2_not = Hotness(100000.0);
                s3_not = Duration::from_secs_f32(0.0);
            } else {
                s1_not = Hotness(
                    tier.files.iter().map(|(_, v)| v.0 .0).sum::<f32>() / tier.files.len() as f32,
                );
                s2_not = Hotness(
                    tier.files
                        .iter()
                        .map(|(_, v)| v.0 .0 * v.1 as f32)
                        .sum::<f32>()
                        / tier.files.len() as f32,
                );
                if tier.reqs.is_empty() {
                    s3_not = Duration::from_secs_f32(0.0);
                    num_reqs = 0;
                } else {
                    num_reqs = tier.reqs.iter().map(|(_, reqs)| reqs.len()).sum::<usize>();
                    s3_not = Duration::from_secs_f32(
                        tier.reqs
                            .iter()
                            .map(|(_, reqs)| {
                                reqs.iter()
                                    .map(|req| req.response_time.as_secs_f32())
                                    .sum::<f32>()
                            })
                            .sum::<f32>()
                            / num_reqs as f32,
                    );
                }
            }

            let (c_not, _) = self.cost_phy(State(s1_not, s2_not, s3_not));

            // Test the possibilities of either adding or removing the file
            if tier.files.contains_key(&obj) {
                // Following is tier_up, which is the value if file is migrated upwards?
                // We remove the file from the tier and try out what changes
                let s1_up;
                let s2_up;
                let s3_up;
                if let Some(dropped) = tier.files.remove(&obj) {
                    s1_up = Hotness(
                        tier.files.iter().map(|(_, v)| v.0 .0).sum::<f32>()
                            / tier.files.len() as f32,
                    );
                    s2_up = Hotness(
                        tier.files
                            .iter()
                            .map(|(_, v)| v.0 .0 * v.1 as f32)
                            .sum::<f32>()
                            / tier.files.len() as f32,
                    );
                    if let Some(dropped_reqs) = tier.reqs.remove(&obj) {
                        if tier.reqs.is_empty() {
                            s3_up = Duration::from_secs_f32(0.0);
                        } else {
                            s3_up = Duration::from_secs_f32(
                                tier.reqs
                                    .iter()
                                    .map(|(_, reqs)| {
                                        reqs.iter()
                                            .map(|req| req.response_time.as_secs_f32())
                                            .sum::<f32>()
                                    })
                                    .sum::<f32>()
                                    / (num_reqs - dropped_reqs.len()) as f32,
                            );
                        }
                        // Clean-up
                        tier.reqs.insert(obj.clone(), dropped_reqs);
                    } else {
                        s3_up = Duration::from_secs_f32(
                            tier.reqs
                                .iter()
                                .map(|(_, reqs)| {
                                    reqs.iter()
                                        .map(|req| req.response_time.as_secs_f32())
                                        .sum::<f32>()
                                })
                                .sum::<f32>()
                                / num_reqs as f32,
                        );
                    }
                    // Clean-up
                    tier.files.insert(obj.clone(), dropped);
                } else {
                    s1_up = Hotness(0.0);
                    s2_up = Hotness(100000.0);
                    s3_up = Duration::from_secs_f32(0.0);
                }

                let (c_up, _) = self.cost_phy(State(s1_up, s2_up, s3_up));
                return (c_not, s1_not, c_up, s1_up);
            } else {
                // TIER does not contain the file, how will it fair if we add it?
                tier.files.insert(obj.clone(), temp);
                // FIXME: Remove the entry again?
                let s1_up = Hotness(
                    tier.files.iter().map(|(_, v)| v.0 .0).sum::<f32>() / tier.files.len() as f32,
                );
                let s2_up = Hotness(
                    tier.files
                        .iter()
                        .map(|(_, v)| v.0 .0 * v.1 as f32)
                        .sum::<f32>()
                        / tier.files.len() as f32,
                );
                let s3_up;
                let num_added_obj_reqs = obj_reqs.len();
                tier.reqs.insert(obj.clone(), obj_reqs);
                if num_added_obj_reqs + num_reqs == 0 {
                    s3_up = Duration::from_secs_f32(0.0);
                } else {
                    s3_up = Duration::from_secs_f32(
                        tier.reqs
                            .iter()
                            .map(|(_, reqs)| {
                                reqs.iter()
                                    .map(|req| req.response_time.as_secs_f32())
                                    .sum::<f32>()
                            })
                            .sum::<f32>()
                            / (num_reqs - num_added_obj_reqs) as f32,
                    );
                }
                // Clean-up
                tier.reqs.remove(&obj);
                tier.files.remove(&obj);

                let (c_up, _) = self.cost_phy(State(s1_up, s2_up, s3_up));
                return (c_not, s1_not, c_up, s1_up);
            }
        }
    }
}

pub struct ZhangHellanderToor {
    tiers: [learning::Tier; NUM_STORAGE_CLASSES],
}
