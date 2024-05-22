use crossbeam_channel::Receiver;
use parking_lot::RwLock;

use crate::{
    cow_bytes::CowBytes,
    data_management::DmlWithStorageHints,
    database::{RootDmu, StorageInfo},
    object::{ObjectStore, ObjectStoreId},
    vdev::Block,
    Database, StoragePreference,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::Write,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use super::{DatabaseMsg, DmlMsg, GlobalObjectId, MigrationConfig, MigrationPolicy};
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
    //! reinforcement learning approach by Zhang et al.
    //!
    //! Most of this module has been translated from the original python
    //! implementation found here <https://github.com/JSFRi/HSM-RL>

    use rand::SeedableRng;
    use std::{
        collections::HashMap,
        ops::Index,
        time::{Duration, Instant},
    };

    use crate::migration::GlobalObjectId;

    pub(super) const EULER: f32 = std::f32::consts::E;

    // How often a file is accessed in certain time range.
    #[derive(Clone, Copy, Debug, Serialize)]
    pub struct Hotness(f32);

    impl Hotness {
        pub(super) fn as_f32(&self) -> f32 {
            self.0
        }
    }

    #[derive(Clone, Debug, Serialize)]
    pub struct Size(u64);

    impl Size {
        pub fn num_bytes(&self) -> u64 {
            self.0
        }
    }

    #[derive(Serialize)]
    pub struct Tier {
        alpha: f32,
        files: HashMap<GlobalObjectId, FileProperties>,
        reqs: HashMap<GlobalObjectId, Vec<Request>>,
        #[serde(skip)]
        rng: rand::rngs::StdRng,
    }

    #[derive(Clone, Serialize)]
    pub struct FileProperties {
        pub hotness: Hotness,
        pub size: Size,
        #[serde(skip)]
        pub last_access: Instant,
    }

    impl Tier {
        pub fn new() -> Self {
            Self {
                alpha: 0.8,
                files: Default::default(),
                reqs: Default::default(),
                rng: rand::rngs::StdRng::seed_from_u64(42),
                // Constant taken from the original implementation
            }
        }

        pub fn size(&self, obj: &GlobalObjectId) -> Option<Size> {
            self.files.get(obj).map(|n| n.size.clone())
        }

        pub fn wipe_requests(&mut self) {
            self.reqs = Default::default();
        }

        pub fn contains(&self, key: &GlobalObjectId) -> bool {
            self.files.contains_key(key)
        }

        pub fn file_info(&self, key: &GlobalObjectId) -> Option<&FileProperties> {
            self.files.get(key)
        }

        pub fn insert(&mut self, key: GlobalObjectId, size: u64) {
            self.files.insert(
                key,
                FileProperties {
                    hotness: Hotness(self.rng.gen_range(0.0..1.0)),
                    size: Size(size),
                    last_access: Instant::now(),
                },
            );
        }

        /// Insert the result of [Self::remove] into a tier again.
        pub fn insert_full(
            &mut self,
            key: GlobalObjectId,
            content: (FileProperties, Option<Vec<Request>>),
        ) {
            self.files.insert(key.clone(), content.0);
            if let Some(reqs) = content.1 {
                self.reqs.insert(key, reqs);
            }
        }

        pub fn remove(
            &mut self,
            key: &GlobalObjectId,
        ) -> Option<(FileProperties, Option<Vec<Request>>)> {
            self.files.remove(key).map(|file_data| {
                // If the files are contained this has to also
                let reqs = self.reqs.remove(key);
                (file_data, reqs)
            })
        }

        pub fn update_size(&mut self, key: &GlobalObjectId, size: u64) {
            if let Some(entry) = self.files.get_mut(key) {
                entry.size = Size(size);
            }
        }

        pub fn coldest(
            &mut self,
        ) -> Option<(GlobalObjectId, (FileProperties, Option<Vec<Request>>))> {
            self.files
                .iter()
                .min_by(|(_, v_left), (_, v_right)| v_left.hotness.0.total_cmp(&v_right.hotness.0))
                .map(|(key, val)| (key.clone(), val.clone()))
                .map(|(key, val)| {
                    // This is already "val"
                    let _ = self.files.remove(&key);
                    let reqs = self.reqs.remove(&key);
                    (key, (val, reqs))
                })
        }

        pub fn msg(&mut self, key: GlobalObjectId, dur: Duration) {
            let time = Instant::now();
            if let Some(elem) = self.files.get_mut(&key) {
                elem.last_access = time;
            }
            self.reqs
                .entry(key)
                .and_modify(|tup| tup.push(Request::new(dur)))
                .or_insert_with(|| vec![Request::new(dur)]);
        }

        // Default beta = 0.05
        pub fn step(&self, beta: f32) -> (State, Reward) {
            // called in original with request dataframe, also calculates s1,s2,s3 of this moment?

            // 1. Calculate reward based on response times of requests
            let mut rewards = 0.0;
            let total = self.reqs.iter().flat_map(|(_, v)| v.iter()).count();
            let mut s3 = Duration::from_secs_f32(0.0);
            for (idx, req) in self.reqs.iter().flat_map(|(_, v)| v.iter()).enumerate() {
                s3 += req.response_time;
                rewards +=
                    req.response_time.as_secs_f32() * EULER.powf(-beta * (idx / total) as f32);
            }
            if total != 0 {
                rewards /= total as f32;
            }

            // 2. Calculate current state
            let s1 = if self.files.is_empty() {
                Hotness(0.0)
            } else {
                Hotness(
                    self.files.values().map(|v| v.hotness.0).sum::<f32>() / self.files.len() as f32,
                )
            };

            let s2 = if self.files.is_empty() {
                Hotness(0.0)
            } else {
                Hotness(
                    self.files
                        .values()
                        .map(|v| v.hotness.0 * v.size.num_bytes() as f32)
                        .sum::<f32>()
                        / self.files.len() as f32,
                )
            };

            (State(s1, s2, s3), Reward(rewards))
        }

        // /// There is a random chance that a file will become hot or cool. This functions provides this chance
        // pub fn hot_cold(&mut self, obj: &ObjectKey) {
        //     if let Some((hotness, _, _, _)) = self.files.get_mut(obj) {
        //         let mut rng = rand::thread_rng();
        //         if hotness.is_hot() {
        //             if rng.gen_bool(Self::COLD_CHANCE) {
        //                 // Downgrade to cool file
        //                 hotness.0 = rng.gen_range(0..5) as f32 / 10.0;
        //             }
        //         } else {
        //             if rng.gen_bool(Self::HOT_CHANCE) {
        //                 // Upgrade to hot file
        //                 hotness.0 = rng.gen_range(6..=10) as f32 / 10.0;
        //             }
        //         }
        //     }
        // }

        // /// Naturally declining temperatures
        // pub fn temp_decrease(&mut self) {
        //     for (key, tup) in self.files.iter_mut() {
        //         if let Some(_) = self.reqs.get(&key) {
        //             tup.2 = 0;
        //         } else {
        //             tup.2 += 1;
        //             if tup.2 % self.decline_step == 0 {
        //                 tup.0 .0 -= 0.1;
        //             }
        //             tup.0.clamp_valid()
        //         }
        //     }
        // }

        pub fn temp_update(&mut self) {
            let time = Instant::now();
            for (_key, tup) in self.files.iter_mut() {
                tup.hotness = Hotness(
                    self.alpha * tup.hotness.0
                        + (1.0 - self.alpha) * (tup.last_access - time).as_secs_f32(),
                );
            }
        }
    }
    use rand::Rng;
    use serde::Serialize;

    #[derive(Clone, Copy, Debug)]
    pub struct Reward(f32);

    /// The state of a single tier.
    #[derive(Clone, Copy, Serialize)]
    pub struct State(Hotness, Hotness, Duration);

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

    pub(super) struct TDAgent {
        p: [f32; 8],
        z: [f32; 8],
        a_i: [f32; 3],
        b_i: [f32; 3],
        alpha: [f32; 8],
        beta: f32,
        lambda: f32,
        phi_list: Vec<[f32; 8]>,
    }

    #[derive(Clone, Debug, Serialize)]
    pub struct Request {
        response_time: Duration,
    }

    impl Request {
        pub fn new(response_time: Duration) -> Self {
            Self { response_time }
        }
    }

    impl TDAgent {
        pub(super) fn new(
            p_init: [f32; 8],
            beta: f32,
            lambda: f32,
            a_i: [f32; 3],
            b_i: [f32; 3],
        ) -> Self {
            Self {
                alpha: [0.0; 8],
                z: [0.0; 8],
                p: p_init,
                a_i,
                b_i,
                beta,
                lambda,
                phi_list: vec![[0.0; 8]],
            }
        }

        pub(super) fn learn(
            &mut self,
            state: State,
            reward: Reward,
            state_next: State,
        ) -> [f32; 8] {
            let (c_n, phi_n) = self.cost_phy(state);
            let (c_n_1, phi) = self.cost_phy(state_next);

            let phi_len = self.phi_list.len();
            for idx in 0..self.alpha.len() {
                self.alpha[idx] = 0.1
                    / (1.0
                        + 100.0
                            * self.phi_list[0..phi_len - 1]
                                .iter()
                                .map(|sub| sub[idx])
                                .sum::<f32>())
            }

            for (idx, z_val) in self.z.iter_mut().enumerate() {
                *z_val = self.lambda * EULER.powf(-self.beta * state.2.as_secs_f32()) * *z_val
                    + phi_n[idx];
            }

            for idx in 0..self.p.len() {
                self.p[idx] += self.alpha[idx]
                    * (reward.0 + EULER.powf(-self.beta * state.2.as_secs_f32()) * c_n_1 - c_n)
                    * self.z[idx];
            }

            self.phi_list.push(phi);
            phi
        }

        fn cost_phy(&self, state: State) -> (f32, [f32; 8]) {
            let membership = state.membership(self);
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
            for (idx, phi_val) in phi.iter().enumerate() {
                c += self.p[idx] * phi_val;
            }
            (c, phi)
        }

        // NOTE: reqs contains requests for this specific tier.
        pub(super) fn c_up_c_not(
            &self,
            tier: &mut Tier,
            obj: GlobalObjectId,
            temp: FileProperties,
            obj_reqs: &Vec<Request>,
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
                num_reqs = 0;
            } else {
                s1_not = Hotness(
                    tier.files.values().map(|v| v.hotness.0).sum::<f32>() / tier.files.len() as f32,
                );
                s2_not = Hotness(
                    tier.files
                        .values()
                        .map(|v| v.hotness.0 * v.size.num_bytes() as f32)
                        .sum::<f32>()
                        / tier.files.len() as f32,
                );
                if tier.reqs.is_empty() {
                    s3_not = Duration::from_secs_f32(0.0);
                    num_reqs = 0;
                } else {
                    num_reqs = tier.reqs.values().map(|reqs| reqs.len()).sum::<usize>();
                    s3_not = Duration::from_secs_f32(
                        tier.reqs
                            .values()
                            .map(|reqs| {
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
                    if tier.files.is_empty() {
                        s1_up = Hotness(0.0);
                        s2_up = Hotness(100000.0);
                    } else {
                        s1_up = Hotness(
                            tier.files.values().map(|v| v.hotness.0).sum::<f32>()
                                / tier.files.len() as f32,
                        );
                        s2_up = Hotness(
                            tier.files
                                .values()
                                .map(|v| v.hotness.0 * v.size.num_bytes() as f32)
                                .sum::<f32>()
                                / tier.files.len() as f32,
                        );
                    }
                    if let Some(dropped_reqs) = tier.reqs.remove(&obj) {
                        if tier.reqs.is_empty() {
                            s3_up = Duration::from_secs_f32(0.0);
                        } else {
                            s3_up = Duration::from_secs_f32(
                                tier.reqs
                                    .values()
                                    .map(|reqs| {
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
                                .values()
                                .map(|reqs| {
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
                (c_not, s1_not, c_up, s1_up)
            } else {
                // TIER does not contain the file, how will it fair if we add it?
                tier.files.insert(obj.clone(), temp);
                let s1_up = Hotness(
                    tier.files.values().map(|v| v.hotness.0).sum::<f32>() / tier.files.len() as f32,
                );
                let s2_up = Hotness(
                    tier.files
                        .values()
                        .map(|v| v.hotness.0 * v.size.num_bytes() as f32)
                        .sum::<f32>()
                        / tier.files.len() as f32,
                );
                let num_added_obj_reqs = obj_reqs.len();
                tier.reqs.insert(obj.clone(), obj_reqs.clone());
                let s3_up = if num_added_obj_reqs + num_reqs == 0 {
                    Duration::from_secs_f32(0.0)
                } else {
                    Duration::from_secs_f32(
                        tier.reqs
                            .values()
                            .map(|reqs| {
                                reqs.iter()
                                    .map(|req| req.response_time.as_secs_f32())
                                    .sum::<f32>()
                            })
                            .sum::<f32>()
                            / (num_reqs + num_added_obj_reqs) as f32,
                    )
                };
                // Clean-up
                tier.reqs.remove(&obj);
                tier.files.remove(&obj);

                let (c_up, _) = self.cost_phy(State(s1_up, s2_up, s3_up));
                (c_not, s1_not, c_up, s1_up)
            }
        }
    }
}

/// Implementation of a reinforcement learning migration policy proposed by
/// Zhang, Hellander and Toor.
///
/// This policy is intended to be used on "uniform" objets which do not use
/// partial speed up, based on user made assumptions of the object.
pub(crate) struct ZhangHellanderToor {
    tiers: Vec<TierAgent>,
    // Stores the most recently known storage location and all requests
    objects: HashMap<GlobalObjectId, ObjectInfo>,
    default_storage_class: StoragePreference,
    config: MigrationConfig<Option<RlConfig>>,
    dml_rx: Receiver<DmlMsg>,
    db_rx: Receiver<DatabaseMsg>,
    delta_moved: Vec<(GlobalObjectId, u64, u8, u8)>,
    state: DatabaseState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Additional configuration for the reinforcement learning policy. These
/// configuration only concern the logging of data for debugging or
/// visualization purposes.
///
/// # To-do
/// - We can also add the "cooling" of files, as in the speed new temperatures
/// are used compared to older existing observations, to the configuration.
pub struct RlConfig {
    /// Path to file which stores the complete recorded state of the storage
    /// stack after each timestep in a newline-delimited json format.
    pub path_state: std::path::PathBuf,
    /// Path to file which stores all migration decisions made by the policy in
    /// a timestep.  Stored as CSV.
    pub path_delta: std::path::PathBuf,
}

/// A convenience struct to manage all the objects easier. And to please the borrow checker.
struct DatabaseState {
    object_stores: HashMap<ObjectStoreId, Option<ObjectStore>>,
    active_storage_classes: u8,
    db: Arc<RwLock<Database>>,
    dmu: Arc<RootDmu>,
}

impl DatabaseState {
    fn get_or_open_object_store<'a>(&'a self, os_id: &ObjectStoreId) -> OsHandle<'a> {
        let os = if let Some(Some(store)) = self.object_stores.get(os_id) {
            store.clone()
        } else {
            let start = std::time::Instant::now();
            let res = self.db.write().open_object_store_with_id(*os_id).unwrap();
            debug!(
                "Opening object stoare took {} ms",
                start.elapsed().as_millis()
            );
            res
        };
        OsHandle {
            state: self,
            act: os,
            os_id: *os_id,
        }
    }

    fn migrate(
        &mut self,
        obj_id: &GlobalObjectId,
        obj_key: &CowBytes,
        target: StoragePreference,
    ) -> super::errors::Result<()> {
        let os = self.get_or_open_object_store(obj_id.store_key());
        let tier_id = target.as_u8() as usize;
        let mut obj = os.act.open_object(obj_key)?.unwrap();
        let start = std::time::Instant::now();
        obj.migrate(target)?;
        debug!("Migrating object took {} ms", start.elapsed().as_millis());
        obj.close()?;
        debug!(
            "Migrating object: {:?} - {} - {tier_id}",
            obj_id,
            tier_id - 1
        );
        Ok(())
    }
}

struct OsHandle<'a> {
    state: &'a DatabaseState,
    act: ObjectStore,
    os_id: ObjectStoreId,
}

impl<'a> Drop for OsHandle<'a> {
    fn drop(&mut self) {
        // NOTE: Object Store should not be open, close quickly..
        if self.state.object_stores.get(&self.os_id).is_none() {
            self.state.db.write().close_object_store(self.act.clone());
        }
    }
}

struct ObjectInfo {
    reqs: Vec<learning::Request>,
    pref: StoragePreference,
    key: CowBytes,
    // An eventual trigger to use when we readjust objects so that we don't move them to their original file back
    probed_lvl: Option<StoragePreference>,
}

impl ObjectInfo {
    fn storage_lvl(&self) -> StoragePreference {
        self.probed_lvl.unwrap_or(self.pref)
    }
}

struct TierAgent {
    tier: learning::Tier,
    agent: learning::TDAgent,
}

const DEFAULT_BETA: f32 = 0.05;
const DEFAULT_LAMBDA: f32 = 0.8;

use std::io::BufWriter;

pub(super) fn open_file_buf_write(
    path: &std::path::PathBuf,
) -> super::errors::Result<BufWriter<std::fs::File>> {
    Ok(BufWriter::new(
        std::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(path)?,
    ))
}

impl ZhangHellanderToor {
    fn timestep(&mut self) -> super::errors::Result<()> {
        // length of tiers
        // saves for each tier the list of possible states?
        let mut tier_results: Vec<learning::State> = vec![];
        // length of tiers
        let mut tier_rewards: Vec<learning::Reward> = vec![];
        for idx in 0..self.state.active_storage_classes {
            let (state, reward) = self.tiers[idx as usize].tier.step(DEFAULT_BETA);
            tier_results.push(state);
            tier_rewards.push(reward);
        }

        for ta in self.tiers.iter_mut() {
            ta.tier.temp_update();
        }

        let active_objects_iter = self
            .objects
            .iter()
            .filter(|(_, obj_info)| !obj_info.reqs.is_empty());

        for (active_obj, obj_data) in active_objects_iter {
            // one tier must exist
            if self.tiers[0].tier.contains(active_obj) {
                continue;
            }

            // NOTE: Instead we iterate to one more level here
            // see comment below
            for tier_id in 1..(self.state.active_storage_classes) as usize {
                if self.tiers[tier_id].tier.contains(active_obj) {
                    // file can be updated or downgraded
                    // self.tiers[tier_id].tier.hot_cold(active_obj);
                    let file_info = self.tiers[tier_id]
                        .tier
                        .file_info(active_obj)
                        .unwrap()
                        .clone();
                    // Calculate changes
                    let obj_reqs = &self.objects.get(active_obj).unwrap().reqs;
                    // Please borrow checker believe me..
                    let tier_agent_upper = &mut self.tiers[tier_id - 1];
                    let (c_not_t_upper, s1_not_t_upper, c_up_t_upper, s1_up_t_upper) =
                        tier_agent_upper.agent.c_up_c_not(
                            &mut tier_agent_upper.tier,
                            active_obj.clone(),
                            file_info.clone(),
                            obj_reqs,
                        );
                    let tier_agent_low = &mut self.tiers[tier_id];
                    let (c_not_t_lower, s1_not_t_lower, c_up_t_lower, s1_up_t_lower) =
                        tier_agent_low.agent.c_up_c_not(
                            &mut tier_agent_low.tier,
                            active_obj.clone(),
                            file_info.clone(),
                            obj_reqs,
                        );

                    // Improvement?
                    if c_up_t_upper * s1_up_t_upper.as_f32() + c_up_t_lower * s1_up_t_lower.as_f32()
                        > c_not_t_upper * s1_not_t_upper.as_f32()
                            + c_not_t_lower * s1_not_t_lower.as_f32()
                    {
                        debug!("Found possible improvement");

                        // NOTE: The original code simply performs an evitction
                        // of too full storage tiers as a manner of downward
                        // migration, we simply pick the lowest temperature for
                        // that.
                        let object_size = Block::from_bytes(
                            self.tiers[tier_id]
                                .tier
                                .size(active_obj)
                                .unwrap()
                                .num_bytes(),
                        )
                        .0;
                        loop {
                            let upper: StorageInfo = self
                                .state
                                .dmu
                                .handler()
                                .free_space_tier
                                .get(tier_id - 1)
                                .unwrap()
                                .into();
                            let lower: StorageInfo = self
                                .state
                                .dmu
                                .handler()
                                .free_space_tier
                                .get(tier_id)
                                .unwrap()
                                .into();

                            if 1.0 - ((upper.free.0 + object_size) as f32 / upper.total.0 as f32)
                                <= self.config.migration_threshold[tier_id - 1]
                            {
                                break;
                            }
                            if let Some(coldest) = self.tiers[tier_id - 1].tier.coldest() {
                                if coldest.1 .0.size.num_bytes() > lower.free.to_bytes() {
                                    warn!("Could not get enough space for file to be migrated downwards");
                                    continue;
                                }
                                // We can move an object from the upper layer
                                // NOTE: Tranfer the object from one to another store.
                                let target = StoragePreference::from_u8(tier_id as u8);
                                let obj_key = &self.objects.get(&coldest.0).unwrap().key;
                                // assume minimum size
                                let _size = Block::from_bytes(coldest.1 .0.size.num_bytes());
                                self.state.migrate(&coldest.0, obj_key, target)?;
                                self.tiers[tier_id]
                                    .tier
                                    .insert_full(coldest.0.clone(), coldest.1.clone());
                                self.delta_moved.push((
                                    coldest.0,
                                    coldest.1 .0.size.num_bytes(),
                                    tier_id as u8 - 1,
                                    tier_id as u8,
                                ));
                            } else {
                                warn!("Migration Daemon could not migrate from full layer as no object was found which inhabits this layer.");
                                warn!("Continuing but functionality may be inhibited.");
                                warn!("Consider using a different policy.");
                                break;
                            }
                        }

                        // NOTE: Migrate new object up
                        let target = StoragePreference::from_u8(tier_id as u8 - 1);
                        self.state.migrate(active_obj, &obj_data.key, target)?;
                        let removed = self.tiers[tier_id].tier.remove(active_obj).unwrap();
                        self.tiers[tier_id - 1]
                            .tier
                            .insert_full(active_obj.clone(), removed);
                        self.delta_moved.push((
                            active_obj.clone(),
                            file_info.size.num_bytes(),
                            tier_id as u8,
                            tier_id as u8 - 1,
                        ));
                    }
                    break;
                }
                continue;
            }

            // NOTE:
            // In the original source code there is a section of code which
            // duplicates exactly what we've done above, but for the lowest
            // tiers, from the comments I assume that this rule intially
            // catapulted data from the last to the first tier, but this
            // behavior has been changed to be more like a gradual upwards
            // migration.
        }

        for tier_id in (1..self.state.active_storage_classes as usize).rev() {
            let mut upper: StorageInfo = self
                .state
                .dmu
                .handler()
                .free_space_tier
                .get(tier_id - 1)
                .unwrap()
                .into();
            let mut lower: StorageInfo = self
                .state
                .dmu
                .handler()
                .free_space_tier
                .get(tier_id)
                .unwrap()
                .into();
            while upper.percent_full() > self.config.migration_threshold[tier_id - 1]
                && lower.percent_full() < self.config.migration_threshold[tier_id]
            {
                debug!("trying to evict full layer");
                // NOTE:
                // In this case we obviously have insufficient info
                // This is due to the uncertainty of objects in the underlying storage, we might end up anywhere
                // TODO: Maybe probing as a quick fix for this, otherwise we
                // need some kind of reporting to the objects in which storage
                // they end up.
                if let Some(coldest) = self.tiers[tier_id - 1].tier.coldest() {
                    if coldest.1 .0.size.num_bytes() > lower.free.to_bytes() {
                        warn!("Could not get enough space for file to be migrated downwards");
                        break;
                    }
                    // We can move an object from the upper layer
                    // NOTE: Tranfer the object from one to another store.
                    self.tiers[tier_id]
                        .tier
                        .insert_full(coldest.0.clone(), coldest.1.clone());
                    let target = StoragePreference::from_u8(tier_id as u8);
                    let obj_key = &self.objects.get(&coldest.0).unwrap().key;
                    self.state.migrate(&coldest.0, obj_key, target)?;
                    self.delta_moved.push((
                        coldest.0,
                        coldest.1 .0.size.num_bytes(),
                        tier_id as u8 - 1,
                        tier_id as u8,
                    ));
                    // self.db().write().sync()?;
                    upper = self
                        .state
                        .dmu
                        .handler()
                        .free_space_tier
                        .get(tier_id - 1)
                        .unwrap()
                        .into();
                    lower = self
                        .state
                        .dmu
                        .handler()
                        .free_space_tier
                        .get(tier_id)
                        .unwrap()
                        .into();
                } else {
                    warn!("Migration Daemon could not migrate from full layer as no object was found which inhabits this layer.");
                    warn!("Continuing but functionality may be inhibited.");
                    warn!("Consider using a different policy.");
                    break;
                }
            }
        }

        for (key, _size, _from, to) in self.delta_moved.iter() {
            let obj = self.objects.get_mut(key).unwrap();
            obj.pref = StoragePreference::from_u8(*to);
            obj.probed_lvl = None;
        }

        // NOTE: Check improvements for each tier and learn agents
        // length of tiers
        for idx in 0..self.state.active_storage_classes {
            let (state, _) = self.tiers[idx as usize].tier.step(DEFAULT_BETA);
            self.tiers[idx as usize].agent.learn(
                tier_results[idx as usize],
                tier_rewards[idx as usize],
                state,
            );
        }

        // // decreasing overall temperatures
        // for ta in self.tiers.iter_mut() {
        //     ta.tier.temp_decrease();
        // }
        Ok(())
    }

    fn cleanup(&mut self) {
        // Clean-up
        for tier in self.tiers.iter_mut() {
            tier.tier.wipe_requests();
        }
        for (_obj, entry) in self.objects.iter_mut() {
            entry.reqs.clear();
        }
        self.delta_moved.clear();
    }

    pub(super) fn build(
        dml_rx: crossbeam_channel::Receiver<super::DmlMsg>,
        db_rx: crossbeam_channel::Receiver<super::DatabaseMsg>,
        db: std::sync::Arc<parking_lot::RwLock<crate::Database>>,
        config: super::MigrationConfig<Option<RlConfig>>,
    ) -> Self {
        // We do not provide single node hints in this policy
        let dmu = Arc::clone(db.read().root_tree.dmu());
        let active_storage_classes = db
            .read()
            .free_space_tier()
            .iter()
            .filter(|tier| tier.free.as_u64() > 0)
            .count() as u8;
        let mut tiers = vec![];
        // Initail parameters
        let b1t1 = 7.33 / 0.3;
        let a1t1 = learning::EULER.powf(b1t1 * 0.3);
        let b2t1 = 7.33 / 1500.0;
        let a2t1 = learning::EULER.powf(b2t1 * 1750.0);
        let b3t1 = 7.33 / 1.0;
        let a3t1 = learning::EULER.powf(b3t1 * 0.5);

        // Alternative Paramters?
        // let b1t2=7.33/0.5;
        // let a1t2=learning::EULER.powf(b1t2*0.55);
        // let b2t2=7.33/1800.0;
        // let a2t2=learning::EULER.powf(b2t2*2500.0);
        // let b3t2=7.33/1.0;
        // let a3t2=learning::EULER.powf(b3t2*0.5);

        // Alternative Paramters?
        // let b1t3=7.33/0.5;
        // let a1t3=learning::EULER.powf(b1t3*0.75);
        // let b2t3=7.33/2000.0;
        // let a2t3=learning::EULER.powf(b2t3*1700.0);
        // let b3t3=7.33/1.0;
        // let a3t3=learning::EULER.powf(b3t3*0.5);

        for _ in 0..active_storage_classes {
            tiers.push(TierAgent {
                tier: learning::Tier::new(),
                agent: learning::TDAgent::new(
                    [0.0; 8],
                    DEFAULT_BETA,
                    DEFAULT_LAMBDA,
                    [a1t1, a2t1, a3t1],
                    [b1t1, b2t1, b3t1],
                ),
            })
        }
        let default_storage_class = dmu.default_storage_class();
        Self {
            dml_rx,
            db_rx,
            tiers,
            default_storage_class,
            config,
            objects: Default::default(),
            delta_moved: Vec::new(),
            state: DatabaseState {
                db,
                dmu,
                active_storage_classes,
                object_stores: Default::default(),
            },
        }
    }
}

impl MigrationPolicy for ZhangHellanderToor {
    // One update call represents one epoch
    fn update(&mut self) -> super::errors::Result<()> {
        // FIXME: This is an inefficient way to get rid of the accumulated
        // messages it would be better to actively close this channel. Fix some
        // behavior in the DML for this.
        for _ in self.dml_rx.try_iter() {}

        for msg in self.db_rx.try_iter() {
            match msg {
                // NOTE: This policy discards dataset information
                DatabaseMsg::DatasetOpen(_) => {}
                DatabaseMsg::DatasetClose(_) => {}

                // Accumulate object stores
                DatabaseMsg::ObjectstoreOpen(key, os) => {
                    self.state.object_stores.insert(key, Some(os));
                }
                DatabaseMsg::ObjectstoreClose(key) => {
                    self.state.object_stores.insert(key, None);
                }
                DatabaseMsg::ObjectOpen(key, info, name)
                | DatabaseMsg::ObjectDiscover(key, info, name) => {
                    if !self.objects.contains_key(&key) {
                        let obj_info = ObjectInfo {
                            reqs: Vec::new(),
                            pref: info.pref,
                            key: name,
                            probed_lvl: None,
                        };
                        // Insert new entry
                        self.objects.insert(key.clone(), obj_info);
                        self.tiers[info.pref.or(self.default_storage_class).as_u8() as usize]
                            .tier
                            .insert(key, info.size);
                    }
                }
                DatabaseMsg::ObjectClose(_, _) => {}
                DatabaseMsg::ObjectRead(key, dur) => {
                    let obj_info = self.objects.get_mut(&key).unwrap();
                    obj_info.reqs.push(learning::Request::new(dur));
                    let pref = obj_info.storage_lvl().or(self.default_storage_class);
                    self.tiers[pref.as_u8() as usize].tier.msg(key, dur);
                }
                DatabaseMsg::ObjectWrite(key, size, _, dur) => {
                    // FIXME: This might again change the storage tier in which the object is stored
                    // This cannot happen on a read operation
                    let obj_info = self.objects.get_mut(&key).unwrap();
                    obj_info.reqs.push(learning::Request::new(dur));
                    // NOTE: Ignore probing for now this has proven to be too erroneous and unreliable
                    // if obj_info.probed_lvl.is_none() {
                    //     let os = self.state.get_or_open_object_store(key.store_key());
                    //     let obj = os.act.open_object(&obj_info.key)?.unwrap();
                    //     obj_info.probed_lvl = Some(obj.probe_storage_level(0)?);
                    // }
                    let pref = obj_info.storage_lvl().or(self.default_storage_class);
                    self.tiers[pref.as_u8() as usize]
                        .tier
                        .update_size(&key, size);
                    self.tiers[pref.as_u8() as usize].tier.msg(key, dur);
                }
                DatabaseMsg::ObjectMigrate(key, pref) => {
                    let prev = self.objects.get_mut(&key).unwrap();
                    if pref != prev.pref {
                        if let Some(info) = self.tiers
                            [prev.pref.or(self.default_storage_class).as_u8() as usize]
                            .tier
                            .remove(&key)
                        {
                            self.tiers[pref.or(self.default_storage_class).as_u8() as usize]
                                .tier
                                .insert_full(key, info);
                        }
                    }
                    prev.pref = pref;
                    prev.probed_lvl = None;
                }
            }
        }
        Ok(())
    }

    fn promote(
        &mut self,
        _storage_tier: u8,
        _tight_space: bool,
    ) -> super::errors::Result<crate::vdev::Block<u64>> {
        unimplemented!()
    }

    fn demote(
        &mut self,
        _storage_tier: u8,
        _desired: crate::vdev::Block<u64>,
    ) -> super::errors::Result<crate::vdev::Block<u64>> {
        unimplemented!()
    }

    fn thread_loop(&mut self) -> super::errors::Result<()> {
        std::thread::sleep(self.config.grace_period);
        loop {
            std::thread::sleep(self.config.update_period);
            let start = std::time::Instant::now();
            debug!("Update");
            self.update()?;
            debug!("Timestep");
            self.timestep()?;
            debug!("Metrics");
            self.metrics()?;
            debug!("Cleanup");
            self.cleanup();
            debug!("Iteration took {} ms", start.elapsed().as_millis());
        }
    }

    fn db(&self) -> &std::sync::Arc<parking_lot::RwLock<crate::Database>> {
        &self.state.db
    }

    fn dmu(&self) -> &std::sync::Arc<RootDmu> {
        &self.state.dmu
    }

    fn config(&self) -> super::MigrationConfig<()> {
        self.config.clone().erased()
    }

    fn metrics(&self) -> super::errors::Result<()> {
        if let Some(p_config) = &self.config.policy_config {
            // Open files
            //
            let mut total_file = open_file_buf_write(&p_config.path_state)?;
            let mut delta_file = open_file_buf_write(&p_config.path_delta)?;

            // Write out state
            //
            // State is new-line delimited json as the rest of the relevant files
            serde_json::to_writer(
                &mut total_file,
                &self
                    .tiers
                    .iter()
                    .map(|lvl| &lvl.tier)
                    .collect::<Vec<&learning::Tier>>(),
            )?;
            total_file.write_all(b"\n")?;
            // Write delta
            //
            let time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            for event in self.delta_moved.iter() {
                delta_file.write_fmt(format_args!(
                    "{},{},{time},{},{}\n",
                    event.0, event.1, event.2, event.3
                ))?;
            }
            delta_file.flush()?;
        }
        Ok(())
    }
}
