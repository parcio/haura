use crossbeam_channel::Receiver;
use parking_lot::RwLock;

use crate::{
    cow_bytes::CowBytes,
    database::DatabaseBuilder,
    object::{ObjectStore, ObjectStoreId},
    storage_pool::NUM_STORAGE_CLASSES,
    Database, StoragePreference,
};
use std::{collections::HashMap, sync::Arc};

use super::{DatabaseMsg, DmlMsg, MigrationConfig, MigrationPolicy, ObjectKey};
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
    //! implementation found here https://github.com/JSFRi/HSM-RL

    use std::{collections::HashMap, ops::Index, time::Duration};

    use crate::{migration::ObjectKey, StoragePreference};

    const EULER: f32 = 2.71828;

    // How often a file is accessed in certain time range.
    #[derive(Clone, Copy)]
    pub struct Hotness(f32);

    impl Hotness {
        fn is_hot(&self) -> bool {
            self.0 > 0.5
        }

        fn clamp_valid(&mut self) {
            self.0 = self.0.clamp(0.1, 1.0)
        }
    }

    pub struct Size(u64);

    impl Size {
        pub fn num_bytes(&self) -> u64 {
            self.0
        }
    }

    pub struct Tier {
        files: HashMap<ObjectKey, (Hotness, Size, u64)>,
        reqs: HashMap<ObjectKey, Vec<Request>>,
        decline_step: u64,
    }

    impl Tier {
        const HOT_CHANCE: f64 = 0.2;
        const COLD_CHANCE: f64 = 0.005;

        pub fn new() -> Self {
            Self {
                files: Default::default(),
                reqs: Default::default(),
                // Constant taken from the original implementation
                decline_step: 10,
            }
        }

        pub fn wipe_requests(&mut self) {
            self.reqs = Default::default();
        }

        pub fn insert(&mut self, key: ObjectKey, size: u64) {
            self.files.insert(key, (Hotness(0.5), Size(size), 0));
        }

        pub fn remove(&mut self, key: &ObjectKey) -> Option<Size> {
            self.files.remove(key).map(|(_, s, _)| s)
        }

        pub fn msg(&mut self, key: ObjectKey, dur: Duration) {
            self.reqs
                .entry(key)
                .and_modify(|mut tup| tup.push(Request::new(dur)))
                .or_insert(vec![Request::new(dur)]);
        }

        // Default beta = 0.05
        pub fn step(&self, beta: f32) -> (State, Reward) {
            // called in original with request dataframe, also calculates s1,s2,s3 of this moment?

            // 1. Calculate reward based on response times of requests
            let mut rewards = 0.0;
            let total = self.reqs.iter().map(|(_, v)| v.iter()).flatten().count();
            let mut s3 = Duration::from_secs_f32(0.0);
            for (idx, req) in self
                .reqs
                .iter()
                .map(|(_, v)| v.iter())
                .flatten()
                .enumerate()
            {
                s3 += req.response_time;
                rewards +=
                    req.response_time.as_secs_f32() * EULER.powf(-beta * (idx / total) as f32);
            }
            rewards /= total as f32;

            // 2. Calculate current state
            let s1;
            if self.files.is_empty() {
                s1 = Hotness(0.0);
            } else {
                s1 = Hotness(
                    self.files.iter().map(|(_, v)| v.0 .0).sum::<f32>() / self.files.len() as f32,
                );
            }

            let s2;
            if self.files.is_empty() {
                s2 = Hotness(0.0);
            } else {
                s2 = Hotness(
                    self.files
                        .iter()
                        .map(|(_, v)| v.0 .0 * v.1.num_bytes() as f32)
                        .sum::<f32>()
                        / self.files.len() as f32,
                );
            }

            return (State(s1, s2, s3), Reward(rewards));
        }

        /// There is a random chance that a file will become hot or cool. This functions provides this chance
        pub fn hot_cold(&mut self, obj: &ObjectKey) {
            if let Some((hotness, _, _)) = self.files.get_mut(obj) {
                let mut rng = rand::thread_rng();
                if hotness.is_hot() {
                    if rng.gen_bool(Self::COLD_CHANCE) {
                        // Downgrade to cool file
                        hotness.0 = rng.gen_range(0..5) as f32 / 10.0;
                    }
                } else {
                    if rng.gen_bool(Self::HOT_CHANCE) {
                        // Upgrade to hot file
                        hotness.0 = rng.gen_range(6..=10) as f32 / 10.0;
                    }
                }
            }
        }

        /// Naturally declining temperatures
        pub fn temp_decrease(&mut self) {
            for (key, tup) in self.files.iter_mut() {
                if let Some(_) = self.reqs.get(&key) {
                    tup.2 = 0;
                } else {
                    tup.2 += 1;
                    if tup.2 % self.decline_step == 0 {
                        tup.0 .0 -= 0.1;
                    }
                    tup.0.clamp_valid()
                }
            }
        }
    }
    use rand::Rng;

    pub struct Reward(f32);

    /// The state of a single tier.
    #[derive(Clone, Copy)]
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

    pub struct Request {
        response_time: Duration,
    }

    impl Request {
        pub fn new(response_time: Duration) -> Self {
            Self { response_time }
        }
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
                self.alpha[idx] = 0.1 / (1.0 + 100.0 * phi_list[idx][..7].iter().sum::<f32>())
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
            tier: &mut Tier,
            obj: ObjectKey,
            temp: (Hotness, Size, u64),
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
                num_reqs = 0;
            } else {
                s1_not = Hotness(
                    tier.files.iter().map(|(_, v)| v.0 .0).sum::<f32>() / tier.files.len() as f32,
                );
                s2_not = Hotness(
                    tier.files
                        .iter()
                        .map(|(_, v)| v.0 .0 * v.1.num_bytes() as f32)
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
                            .map(|(_, v)| v.0 .0 * v.1.num_bytes() as f32)
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
                        .map(|(_, v)| v.0 .0 * v.1.num_bytes() as f32)
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

// FIXME: Add agent
pub struct ZhangHellanderToor<C: DatabaseBuilder + Clone> {
    tiers: [learning::Tier; NUM_STORAGE_CLASSES],
    object_stores: HashMap<ObjectStoreId, Option<ObjectStore<C>>>,
    // Stores the most recently known storage location and all requests
    objects: HashMap<ObjectKey, (Vec<learning::Request>, StoragePreference, CowBytes)>,
    config: MigrationConfig<()>,
    dml_rx: Receiver<DmlMsg>,
    db_rx: Receiver<DatabaseMsg<C>>,
    db: Arc<RwLock<Database<C>>>,
    dmu: Arc<<C as DatabaseBuilder>::Dmu>,
}

impl<C: DatabaseBuilder + Clone> MigrationPolicy<C> for ZhangHellanderToor<C> {
    type Config = ();

    fn build(
        dml_rx: crossbeam_channel::Receiver<super::DmlMsg>,
        db_rx: crossbeam_channel::Receiver<super::DatabaseMsg<C>>,
        db: std::sync::Arc<parking_lot::RwLock<crate::Database<C>>>,
        config: super::MigrationConfig<Self::Config>,
        storage_hint_sink: std::sync::Arc<
            parking_lot::Mutex<HashMap<crate::storage_pool::DiskOffset, crate::StoragePreference>>,
        >,
    ) -> Self {
        // We do not provide single node hints in this policy
        let dmu = Arc::clone(db.read().root_tree.dmu());
        Self {
            dml_rx,
            db_rx,
            db,
            dmu,
            tiers: [(); NUM_STORAGE_CLASSES].map(|_| learning::Tier::new()),
            config,
            object_stores: Default::default(),
            objects: Default::default(),
        }
    }

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
                    self.object_stores.insert(key, Some(os));
                }
                DatabaseMsg::ObjectstoreClose(key) => {
                    self.object_stores.insert(key, None);
                }
                DatabaseMsg::ObjectOpen(key, info, name)
                | DatabaseMsg::ObjectDiscover(key, info, name) => {
                    // Add the object to the belonging tier
                    // Does this count as an operation? Not really if we define it as some modification operations, have a look at the paper
                    if let Some(prev) = self.objects.get_mut(&key) {
                        if prev.1 != info.pref {
                            self.tiers[prev.1.as_u8() as usize].remove(&key);
                            self.tiers[info.pref.as_u8() as usize].insert(key, info.size);
                        }
                        prev.1 = info.pref;
                        prev.2 = name;
                    } else {
                        // Insert new entry
                        self.objects.insert(key, (Vec::new(), info.pref, name));
                    }
                }
                DatabaseMsg::ObjectClose(_, _) => todo!(),
                DatabaseMsg::ObjectRead(key, dur) => {
                    let mut obj_info = self.objects.get_mut(&key).unwrap();
                    obj_info.0.push(learning::Request::new(dur.clone()));
                    self.tiers[obj_info.1.as_u8() as usize].msg(key, dur);
                }
                DatabaseMsg::ObjectWrite(key, _, _, dur) => {
                    // FIXME: This might again change the storage tier in which the object is stored
                    // This cannot happen on a read operation
                    let mut obj_info = self.objects.get_mut(&key).unwrap();
                    obj_info.0.push(learning::Request::new(dur.clone()));
                    self.tiers[obj_info.1.as_u8() as usize].msg(key, dur);
                }
                DatabaseMsg::ObjectMigrate(key, pref) => {
                    let prev = self.objects.get_mut(&key).unwrap();
                    if pref != prev.1 {
                        let size = self.tiers[prev.1.as_u8() as usize].remove(&key).unwrap();
                        self.tiers[pref.as_u8() as usize].insert(key, size.num_bytes());
                    }
                    prev.1 = pref;
                }
            }
        }

        // Perform learning and alike

        // Clean-up
        for tier in self.tiers.iter_mut() {
            tier.wipe_requests();
        }
        for (obj, entry) in self.objects.iter_mut() {
            entry.0 = Default::default();
        }
        todo!()
    }

    fn promote(&mut self, storage_tier: u8) -> super::errors::Result<crate::vdev::Block<u32>> {
        todo!()
    }

    fn demote(
        &mut self,
        storage_tier: u8,
        desired: crate::vdev::Block<u64>,
    ) -> super::errors::Result<crate::vdev::Block<u64>> {
        todo!()
    }

    fn db(&self) -> &std::sync::Arc<parking_lot::RwLock<crate::Database<C>>> {
        &self.db
    }

    fn dmu(&self) -> &std::sync::Arc<<C as DatabaseBuilder>::Dmu> {
        &self.dmu
    }

    fn config(&self) -> &super::MigrationConfig<Self::Config> {
        &self.config
    }
}
