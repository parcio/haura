use crossbeam_channel::Receiver;
use parking_lot::RwLock;

use crate::{
    cow_bytes::CowBytes,
    data_management::DmlWithStorageHints,
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

    pub(super) const EULER: f32 = 2.71828;

    // How often a file is accessed in certain time range.
    #[derive(Clone, Copy, Debug)]
    pub struct Hotness(f32);

    impl Hotness {
        fn is_hot(&self) -> bool {
            self.0 > 0.5
        }

        fn clamp_valid(&mut self) {
            self.0 = self.0.clamp(0.1, 1.0)
        }

        pub(super) fn as_f32(&self) -> f32 {
            self.0
        }
    }

    #[derive(Clone, Debug)]
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

        pub fn contains(&self, key: &ObjectKey) -> bool {
            self.files.contains_key(key)
        }

        pub fn file_info(&self, key: &ObjectKey) -> Option<&(Hotness, Size, u64)> {
            self.files.get(key)
        }

        pub fn update_or_not(&mut self, key: ObjectKey, entry: (Hotness, Size, u64)) {
            if self.files.contains_key(&key) {
                self.remove(&key);
            } else {
                self.insert_with_values(key, entry)
            }
        }

        pub(super) fn insert_with_values(&mut self, key: ObjectKey, entry: (Hotness, Size, u64)) {
            self.files.insert(key, entry);
        }

        pub fn insert(&mut self, key: ObjectKey, size: u64) {
            self.files.insert(key, (Hotness(0.5), Size(size), 0));
        }

        pub fn remove(&mut self, key: &ObjectKey) -> Option<Size> {
            self.files.remove(key).map(|(_, s, _)| s)
        }

        pub fn update_size(&mut self, key: &ObjectKey, size: u64) {
            self.files.get_mut(key).map(|entry| entry.1 = Size(size));
        }

        pub fn coldest(&mut self) -> Option<(ObjectKey, (Hotness, Size, u64))> {
            self.files
                .iter()
                .min_by(|(_, v_left), (_, v_right)| v_left.0 .0.total_cmp(&v_right.0 .0))
                .map(|(key, val)| (key.clone(), val.clone()))
                .and_then(|(key, val)| {
                    self.files.remove(&key);
                    Some((key, val))
                })
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
            if total != 0 {
                rewards /= total as f32;
            }

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

    #[derive(Clone, Copy, Debug)]
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

    enum Action {
        Promote(),
        Demote(),
    }

    #[derive(Clone, Debug)]
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

            for idx in 0..self.z.len() {
                self.z[idx] =
                    self.lambda * EULER.powf(-self.beta * state.2.as_secs_f32()) * self.z[idx]
                        + phi_n[idx];
            }

            for idx in 0..self.p.len() {
                self.p[idx] = self.p[idx]
                    + self.alpha[idx]
                        * (reward.0 + EULER.powf(-self.beta * state.2.as_secs_f32()) * c_n_1 - c_n)
                        * self.z[idx];
            }

            self.phi_list.push(phi.clone());
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
        pub(super) fn c_up_c_not(
            &self,
            tier: &mut Tier,
            obj: ObjectKey,
            temp: (Hotness, Size, u64),
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
                    if tier.files.is_empty() {
                        s1_up = Hotness(0.0);
                        s2_up = Hotness(100000.0);
                    } else {
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
                    }
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
                tier.reqs.insert(obj.clone(), obj_reqs.clone());
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
                            / (num_reqs + num_added_obj_reqs) as f32,
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

pub struct ZhangHellanderToor<C: DatabaseBuilder + Clone> {
    tiers: Vec<TierAgent>,
    object_stores: HashMap<ObjectStoreId, Option<ObjectStore<C>>>,
    // Stores the most recently known storage location and all requests
    objects: HashMap<ObjectKey, (Vec<learning::Request>, StoragePreference, CowBytes)>,
    active_storage_classes: u8,
    default_storage_class: StoragePreference,
    config: MigrationConfig<()>,
    dml_rx: Receiver<DmlMsg>,
    db_rx: Receiver<DatabaseMsg<C>>,
    db: Arc<RwLock<Database<C>>>,
    dmu: Arc<<C as DatabaseBuilder>::Dmu>,
}

struct TierAgent {
    tier: learning::Tier,
    agent: learning::TDAgent,
}

const DEFAULT_BETA: f32 = 0.05;
const DEFAULT_LAMBDA: f32 = 0.8;

impl<C: DatabaseBuilder + Clone> ZhangHellanderToor<C> {
    fn get_or_open_object_store(&self, os_id: &ObjectStoreId) -> ObjectStore<C> {
        if let Some(Some(store)) = self.object_stores.get(os_id) {
            store.clone()
        } else {
            self.db
                .write()
                .open_object_store_with_id(os_id.clone())
                .unwrap()
        }
    }

    fn timestep(&mut self) -> super::errors::Result<()> {
        // length of tiers
        // saves for each tier the list of possible states?
        let mut tier_results: Vec<learning::State> = vec![];
        // length of tiers
        let mut tier_rewards: Vec<learning::Reward> = vec![];
        for idx in 0..self.active_storage_classes {
            let (state, reward) = self.tiers[idx as usize].tier.step(DEFAULT_BETA);
            tier_results.push(state);
            tier_rewards.push(reward);
        }

        let mut request_weight = 0;
        let mut request_large = 0;

        let active_objects_iter = self.objects.iter().filter(|(_, req)| req.0.len() > 0);

        for (active_obj, obj_data) in active_objects_iter {
            // one tier must exist
            if self.tiers[0].tier.contains(active_obj) {
                // file in fastest tier
                self.tiers[0].tier.hot_cold(active_obj);
                let file_info = self.tiers[0].tier.file_info(active_obj).unwrap();

                request_weight += file_info.1.num_bytes();
                // magic 5000
                if file_info.1.num_bytes() > 5000 {
                    request_large += 1
                }
                continue;
            }

            // NOTE: Instead we iterate to one more level here
            // see comment below
            for tier_id in 1..(self.active_storage_classes) as usize {
                if self.tiers[tier_id].tier.contains(active_obj) {
                    // file can be updated or downgraded
                    self.tiers[tier_id].tier.hot_cold(active_obj);
                    let file_info = self.tiers[tier_id]
                        .tier
                        .file_info(active_obj)
                        .unwrap()
                        .clone();
                    request_weight += file_info.1.num_bytes();
                    // magic 5000
                    if file_info.1.num_bytes() > 5000 {
                        request_large += 1
                    }

                    // Calculate changes
                    let obj_reqs = &self.objects.get(active_obj).unwrap().0;
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
                        let free_space = self.db.read().free_space_tier();

                        // NOTE: The original code simply performs an evitction
                        // of too full storage tiers as a manner of downward
                        // migration, we simply pick the lowest temperature for
                        // that.
                        if ((free_space[tier_id - 1].total.as_u64() as f32
                            * self.config.migration_threshold) as u64)
                            > free_space[tier_id - 1].free.as_u64()
                        {
                            // if there is not enough space left migrate down
                            // get coldest file
                            if let Some(coldest) = self.tiers[tier_id - 1].tier.coldest() {
                                if coldest.1 .1.num_bytes() > free_space[tier_id].free.to_bytes() {
                                    warn!("Could not get enough space for file to be migrated downwards");
                                    continue;
                                }
                                // We can move an object from the upper layer
                                // NOTE: Tranfer the object from one to another store.
                                self.tiers[tier_id - 1].tier.remove(&coldest.0);
                                self.tiers[tier_id]
                                    .tier
                                    .insert_with_values(coldest.0.clone(), coldest.1);
                                let target = StoragePreference::from_u8(tier_id as u8);
                                let os = self.get_or_open_object_store(coldest.0.store_key());
                                let obj_key = &self.objects.get(&coldest.0).unwrap().2;
                                let mut obj = os.open_object(obj_key)?.unwrap();
                                obj.migrate(target)?;
                                obj.close()?;
                                // NOTE: Object Store should not be open, close quickly..
                                if let None = self.object_stores.get(coldest.0.store_key()) {
                                    self.db.write().close_object_store(os);
                                }
                                // self.db.write().close_object_store(os);
                                debug!(
                                    "Migrating object: {:?} - {} - {tier_id}",
                                    coldest.0,
                                    tier_id - 1
                                );
                            } else {
                                warn!("Migration Daemon could not migrate from full layer as no object was found which inhabits this layer.");
                                warn!("Continuing but functionality may be inhibited.");
                                warn!("Consider using a different policy.");
                                continue;
                            }
                        }

                        // NOTE: Migrate new object up
                        let target = StoragePreference::from_u8(tier_id as u8 - 1);
                        let os = self.get_or_open_object_store(active_obj.store_key());
                        if let Some(mut obj) = os.open_object(&obj_data.2)? {
                            obj.migrate(target)?;
                            debug!(
                                "Migrating object: {:?} - {tier_id} - {}",
                                active_obj,
                                tier_id - 1
                            );
                            obj.close()?;
                            // NOTE: Tranfer the object from one to another store.
                            self.tiers[tier_id].tier.remove(&active_obj);
                            self.tiers[tier_id - 1]
                                .tier
                                .insert_with_values(active_obj.clone(), file_info.clone());
                        }
                        // NOTE: Object Store should not be open, close quickly..
                        if let None = self.object_stores.get(active_obj.store_key()) {
                            self.db.write().close_object_store(os);
                        }
                        // self.db.write().close_object_store(os);
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

        // NOTE: Check improvements for each tier and learn agents
        // length of tiers
        for idx in 0..self.active_storage_classes {
            let (state, _) = self.tiers[idx as usize].tier.step(DEFAULT_BETA);
            self.tiers[idx as usize].agent.learn(
                tier_results[idx as usize],
                tier_rewards[idx as usize],
                state,
            );
        }

        let mut free_space = self.db.read().free_space_tier();

        for tier_id in 1..self.active_storage_classes as usize {
            while (free_space[tier_id - 1].free.as_u64() as f32)
                < free_space[tier_id - 1].total.as_u64() as f32 * self.config.migration_threshold
            {
                debug!("trying to evict full layer");
                // NOTE:
                // In this case we obviously have insufficient info
                // This is due to the uncertainty of objects in the underlying storage, we might end up anywhere
                // TODO: Maybe probing as a quick fix for this, otherwise we
                // need some kind of reporting to the objects in which storage
                // they end up.
                if let Some(coldest) = self.tiers[tier_id - 1].tier.coldest() {
                    if coldest.1 .1.num_bytes() > free_space[tier_id].free.to_bytes() {
                        warn!("Could not get enough space for file to be migrated downwards");
                        break;
                    }
                    // We can move an object from the upper layer
                    // NOTE: Tranfer the object from one to another store.
                    self.tiers[tier_id - 1].tier.remove(&coldest.0);
                    self.tiers[tier_id]
                        .tier
                        .insert_with_values(coldest.0.clone(), coldest.1);
                    let target = StoragePreference::from_u8(tier_id as u8);
                    let os = self.get_or_open_object_store(coldest.0.store_key());
                    let obj_key = &self.objects.get(&coldest.0).unwrap().2;
                    let mut obj = os.open_object(obj_key)?.unwrap();
                    obj.migrate(target)?;
                    obj.close()?;
                    // NOTE: Object Store should not be open, close quickly..
                    if let None = self.object_stores.get(coldest.0.store_key()) {
                        self.db.write().close_object_store(os);
                    }
                    debug!(
                        "Migrating object: {:?} - {} - {tier_id}",
                        coldest.0,
                        tier_id - 1
                    );
                    free_space = self.db.read().free_space_tier();
                } else {
                    warn!("Migration Daemon could not migrate from full layer as no object was found which inhabits this layer.");
                    warn!("Continuing but functionality may be inhibited.");
                    warn!("Consider using a different policy.");
                    break;
                }
            }
        }

        // decreasing overall temperatures
        for ta in self.tiers.iter_mut() {
            ta.tier.temp_decrease();
        }

        // Clean-up
        for tier in self.tiers.iter_mut() {
            tier.tier.wipe_requests();
        }
        for (_obj, entry) in self.objects.iter_mut() {
            entry.0 = Default::default();
        }
        Ok(())
    }

    pub(super) fn build(
        dml_rx: crossbeam_channel::Receiver<super::DmlMsg>,
        db_rx: crossbeam_channel::Receiver<super::DatabaseMsg<C>>,
        db: std::sync::Arc<parking_lot::RwLock<crate::Database<C>>>,
        config: super::MigrationConfig<()>,
        _storage_hint_sink: std::sync::Arc<
            parking_lot::Mutex<HashMap<crate::storage_pool::DiskOffset, crate::StoragePreference>>,
        >,
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
            db,
            dmu,
            tiers,
            active_storage_classes,
            default_storage_class,
            config,
            object_stores: Default::default(),
            objects: Default::default(),
        }
    }
}

impl<C: DatabaseBuilder + Clone> MigrationPolicy<C> for ZhangHellanderToor<C> {
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
                    // FIXME: Add the object to the belonging tier
                    // Does this count as an operation? Not really if we define it as some modification operations, have a look at the paper
                    if let Some(prev) = self.objects.get_mut(&key) {
                        if prev.1 != info.pref {
                            self.tiers[prev.1.or(self.default_storage_class).as_u8() as usize]
                                .tier
                                .remove(&key);
                            self.tiers[info.pref.or(self.default_storage_class).as_u8() as usize]
                                .tier
                                .insert(key, info.size);
                        }
                        prev.1 = info.pref;
                        prev.2 = name;
                    } else {
                        // Insert new entry
                        self.objects
                            .insert(key.clone(), (Vec::new(), info.pref, name));
                        self.tiers[info.pref.or(self.default_storage_class).as_u8() as usize]
                            .tier
                            .insert(key, info.size);
                    }
                }
                DatabaseMsg::ObjectClose(_, _) => {}
                DatabaseMsg::ObjectRead(key, dur) => {
                    let obj_info = self.objects.get_mut(&key).unwrap();
                    obj_info.0.push(learning::Request::new(dur.clone()));
                    self.tiers[obj_info.1.or(self.default_storage_class).as_u8() as usize]
                        .tier
                        .msg(key, dur);
                }
                DatabaseMsg::ObjectWrite(key, size, _, dur) => {
                    // FIXME: This might again change the storage tier in which the object is stored
                    // This cannot happen on a read operation
                    let obj_info = self.objects.get_mut(&key).unwrap();
                    obj_info.0.push(learning::Request::new(dur.clone()));
                    self.tiers[obj_info.1.or(self.default_storage_class).as_u8() as usize]
                        .tier
                        .update_size(&key, size);
                    self.tiers[obj_info.1.or(self.default_storage_class).as_u8() as usize]
                        .tier
                        .msg(key, dur);
                }
                DatabaseMsg::ObjectMigrate(key, pref) => {
                    let prev = self.objects.get_mut(&key).unwrap();
                    if pref != prev.1 {
                        if let Some(size) = self.tiers
                            [prev.1.or(self.default_storage_class).as_u8() as usize]
                            .tier
                            .remove(&key)
                        {
                            self.tiers[pref.or(self.default_storage_class).as_u8() as usize]
                                .tier
                                .insert(key, size.num_bytes());
                        }
                    }
                    prev.1 = pref;
                }
            }
        }
        Ok(())
    }

    fn promote(&mut self, storage_tier: u8) -> super::errors::Result<crate::vdev::Block<u32>> {
        unimplemented!()
    }

    fn demote(
        &mut self,
        storage_tier: u8,
        desired: crate::vdev::Block<u64>,
    ) -> super::errors::Result<crate::vdev::Block<u64>> {
        unimplemented!()
    }

    fn thread_loop(&mut self) -> super::errors::Result<()> {
        std::thread::sleep(self.config.grace_period);
        loop {
            std::thread::sleep(self.config.update_period);
            self.update()?;
            self.timestep()?;
        }
    }

    fn db(&self) -> &std::sync::Arc<parking_lot::RwLock<crate::Database<C>>> {
        &self.db
    }

    fn dmu(&self) -> &std::sync::Arc<<C as DatabaseBuilder>::Dmu> {
        &self.dmu
    }

    fn config(&self) -> super::MigrationConfig<()> {
        self.config.erased()
    }
}
