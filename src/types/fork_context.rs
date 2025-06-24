use parking_lot::RwLock;
use std::{collections::BTreeMap, sync::Arc};

use helper_functions::misc;
use std::collections::HashMap;
use types::{
    config::Config,
    nonstandard::Phase,
    phase0::{
        consts::{FAR_FUTURE_EPOCH, GENESIS_EPOCH},
        primitives::{Epoch, ForkDigest, Slot, H256},
    },
    preset::Preset,
};

/// Provides fork specific info like the current phase and the fork digests corresponding to every valid fork.
#[derive(Debug)]
pub struct ForkContext {
    chain_config: Arc<Config>,
    current_fork: RwLock<Phase>,
    current_fork_digest: RwLock<ForkDigest>,
    next_fork_digest: RwLock<ForkDigest>,
    fork_epoch_to_digest: BTreeMap<Epoch, ForkDigest>,
    digest_to_fork_epoch: HashMap<ForkDigest, Epoch>,
}

impl ForkContext {
    /// Creates a new `ForkContext` object by enumerating all enabled forks and computing their
    /// fork digest.
    pub fn new<P: Preset>(
        config: &Arc<Config>,
        current_slot: Slot,
        genesis_validators_root: H256,
    ) -> Self {
        let fork_epoch_to_digest = enum_iterator::all::<Phase>()
            .filter(|phase| config.is_phase_enabled::<P>(*phase))
            .map(|phase| config.fork_epoch(phase))
            .chain(config.blob_schedule.iter().map(|entry| entry.epoch))
            .map(|epoch| {
                let digest = misc::compute_fork_digest(config, genesis_validators_root, epoch);
                (epoch, digest)
            })
            .collect::<BTreeMap<_, _>>();

        let digest_to_fork_epoch = fork_epoch_to_digest.iter().map(|(k, v)| (*v, *k)).collect();
        let current_fork = RwLock::new(config.phase_at_slot::<P>(current_slot));
        let current_epoch = misc::compute_epoch_at_slot::<P>(current_slot);
        let current_fork_digest = fork_epoch_to_digest
            .iter()
            .rev()
            .find_map(|(epoch, fork_digest)| (*epoch <= current_epoch).then_some(*fork_digest))
            .unwrap_or_default();
        let next_fork_digest = fork_epoch_to_digest
            .iter()
            .find_map(|(epoch, fork_digest)| (*epoch > current_epoch).then_some(*fork_digest))
            .unwrap_or_default();

        Self {
            chain_config: config.clone(),
            current_fork,
            current_fork_digest: RwLock::new(current_fork_digest),
            next_fork_digest: RwLock::new(next_fork_digest),
            fork_epoch_to_digest,
            digest_to_fork_epoch,
        }
    }

    /// Returns a dummy fork context for testing.
    pub fn dummy<P: Preset>(config: &Arc<Config>, phase: Phase) -> ForkContext {
        let current_slot = config
            .fork_slot::<P>(phase)
            .expect("all phases should be enabled in configuration");

        Self::new::<P>(config, current_slot, H256::zero())
    }

    /// Returns `true` if the provided `phase` exists in the `ForkContext` object.
    pub fn fork_exists(&self, phase: Phase) -> bool {
        let fork_epoch = self.chain_config.fork_epoch(phase);
        self.fork_epoch_to_digest.contains_key(&fork_epoch)
    }

    /// Returns the `current_fork`.
    pub fn current_fork(&self) -> Phase {
        *self.current_fork.read()
    }

    /// Updates the `current_fork` field to a new fork.
    pub fn update_current_fork(&self, new_fork: Phase) {
        *self.current_fork.write() = new_fork;
    }

    /// Returns the `current_fork_digest`.
    pub fn current_fork_digest(&self) -> ForkDigest {
        *self.current_fork_digest.read()
    }

    /// Updates the `current_fork_digest` field to a new fork digest.
    pub fn update_current_fork_digest(&self, new_fork_digest: ForkDigest) {
        *self.current_fork_digest.write() = new_fork_digest;
    }

    /// Returns the `next_fork_digest`.
    pub fn next_fork_digest(&self) -> ForkDigest {
        *self.next_fork_digest.read()
    }

    /// Updates the `next_fork_digest` field to a new fork digest.
    pub fn update_next_fork_digest(&self, new_fork_digest: ForkDigest) {
        *self.next_fork_digest.write() = new_fork_digest;
    }

    /// Returns the context bytes/fork_digest corresponding to the genesis fork version.
    pub fn genesis_context_bytes(&self) -> ForkDigest {
        *self
            .fork_epoch_to_digest
            .get(&GENESIS_EPOCH)
            .expect("ForkContext must contain genesis context bytes")
    }

    /// Returns the fork type given the context bytes/fork_digest.
    /// Returns `None` if context bytes doesn't correspond to any valid `Phase`.
    pub fn from_context_bytes(&self, context: ForkDigest) -> Option<Phase> {
        self.digest_to_fork_epoch
            .get(&context)
            .map(|fork_epoch| self.chain_config.phase_at_epoch(*fork_epoch))
    }

    /// Returns the context bytes/fork_digest corresponding to a fork name.
    /// Returns `None` if the `Phase` has not been initialized.
    pub fn to_context_bytes(&self, phase: Phase) -> Option<ForkDigest> {
        let fork_epoch = self.chain_config.fork_epoch(phase);
        self.fork_epoch_to_digest.get(&fork_epoch).cloned()
    }

    /// Returns all `fork_digest`s that are currently in the `ForkContext` object.
    pub fn all_fork_digests(&self) -> Vec<ForkDigest> {
        self.digest_to_fork_epoch.keys().cloned().collect()
    }

    pub fn chain_config(&self) -> &Arc<Config> {
        &self.chain_config
    }

    pub fn next_fork(&self, current_epoch: Epoch) -> (Epoch, ForkDigest) {
        self.fork_epoch_to_digest
            .iter()
            .find_map(|(epoch, fork_digest)| {
                (*epoch > current_epoch).then_some((*epoch, *fork_digest))
            })
            .unwrap_or((FAR_FUTURE_EPOCH, ForkDigest::default()))
    }

    pub fn fork_digest_at_epoch(&self, epoch: Epoch) -> Option<&ForkDigest> {
        self.fork_epoch_to_digest.get(&epoch)
    }

    pub fn context_bytes_at_epoch(&self, epoch: Epoch) -> ForkDigest {
        self.fork_epoch_to_digest
            .iter()
            .rev()
            .find_map(|(fork_epoch, fork_digest)| (*fork_epoch <= epoch).then_some(*fork_digest))
            .unwrap_or_default()
    }

    pub fn all_fork_epochs(&self) -> Vec<Epoch> {
        self.fork_epoch_to_digest.keys().cloned().collect()
    }

    pub fn current_fork_epoch(&self) -> Epoch {
        self.digest_to_fork_epoch
            .get(&self.current_fork_digest())
            .copied()
            .unwrap_or_default()
    }

    pub fn next_fork_at_slot<P: Preset>(&self, slot: Slot) -> Option<(&Epoch, &ForkDigest)> {
        self.fork_epoch_to_digest
            .iter()
            .find(|(fork_epoch, _)| slot < misc::compute_start_slot_at_epoch::<P>(**fork_epoch))
    }
}
