//! A collection of variables that are accessible outside of the network thread itself.
use crate::peer_manager::peerdb::PeerDB;
use crate::rpc::{MetaData, MetaDataV3};
use crate::types::{BackFillState, SyncState};
use crate::{Client, Eth2Enr};
use crate::{Enr, GossipTopic, Multiaddr, PeerId};
use crate::EnrExt;
use ethereum_types::U256;
use helper_functions::misc;
use parking_lot::RwLock;
use ssz::Uint256;
use types::eip7594::CUSTODY_REQUIREMENT;
use std::collections::HashSet;
use types::{eip7594::ColumnIndex, phase0::primitives::SubnetId};

pub struct NetworkGlobals {
    /// The current local ENR.
    pub local_enr: RwLock<Enr>,
    /// The local peer_id.
    pub peer_id: RwLock<PeerId>,
    /// Listening multiaddrs.
    pub listen_multiaddrs: RwLock<Vec<Multiaddr>>,
    /// The collection of known peers.
    pub peers: RwLock<PeerDB>,
    // The local meta data of our node.
    pub local_metadata: RwLock<MetaData>,
    /// The current gossipsub topic subscriptions.
    pub gossipsub_subscriptions: RwLock<HashSet<GossipTopic>>,
    /// The current sync status of the node.
    pub sync_state: RwLock<SyncState>,
    /// The current state of the backfill sync.
    pub backfill_state: RwLock<BackFillState>,
}

impl NetworkGlobals {
    pub fn new(
        enr: Enr,
        local_metadata: MetaData,
        trusted_peers: Vec<PeerId>,
        disable_peer_scoring: bool,
        log: &slog::Logger,
    ) -> Self {
        NetworkGlobals {
            local_enr: RwLock::new(enr.clone()),
            peer_id: RwLock::new(enr.peer_id()),
            listen_multiaddrs: RwLock::new(Vec::new()),
            local_metadata: RwLock::new(local_metadata),
            peers: RwLock::new(PeerDB::new(trusted_peers, disable_peer_scoring, log)),
            gossipsub_subscriptions: RwLock::new(HashSet::new()),
            sync_state: RwLock::new(SyncState::Stalled),
            backfill_state: RwLock::new(BackFillState::NotRequired),
        }
    }

    /// Returns the local ENR from the underlying Discv5 behaviour that external peers may connect
    /// to.
    pub fn local_enr(&self) -> Enr {
        self.local_enr.read().clone()
    }

    /// Returns the local libp2p PeerID.
    pub fn local_peer_id(&self) -> PeerId {
        *self.peer_id.read()
    }

    /// Returns the list of `Multiaddr` that the underlying libp2p instance is listening on.
    pub fn listen_multiaddrs(&self) -> Vec<Multiaddr> {
        self.listen_multiaddrs.read().clone()
    }

    /// Returns the number of libp2p connected peers.
    pub fn connected_peers(&self) -> usize {
        self.peers.read().connected_peer_ids().count()
    }

    /// Check if peer is connected
    pub fn is_peer_connected(&self, peer_id: &PeerId) -> bool {
        self.peers.read().is_peer_connected(peer_id)
    }

    /// Returns the number of libp2p connected peers with outbound-only connections.
    pub fn connected_outbound_only_peers(&self) -> usize {
        self.peers.read().connected_outbound_only_peers().count()
    }

    /// Returns the number of libp2p peers that are either connected or being dialed.
    pub fn connected_or_dialing_peers(&self) -> usize {
        self.peers.read().connected_or_dialing_peers().count()
    }

    /// Returns in the node is syncing.
    pub fn is_syncing(&self) -> bool {
        self.sync_state.read().is_syncing()
    }

    /// Returns the current sync state of the peer.
    pub fn sync_state(&self) -> SyncState {
        self.sync_state.read().clone()
    }

    /// Returns the current backfill state.
    pub fn backfill_state(&self) -> BackFillState {
        self.backfill_state.read().clone()
    }

    /// Returns a `Client` type if one is known for the `PeerId`.
    pub fn client(&self, peer_id: &PeerId) -> Client {
        self.peers
            .read()
            .peer_info(peer_id)
            .map(|info| info.client().clone())
            .unwrap_or_default()
    }

    /// Updates the syncing state of the node.
    ///
    /// The old state is returned
    pub fn set_sync_state(&self, new_state: SyncState) -> SyncState {
        std::mem::replace(&mut *self.sync_state.write(), new_state)
    }

    /// Get custody subnet count from Metadata cache. 
    /// if not available, get from `csc` field of ENR object instead.
    pub fn custody_subnet_count(&self, enr: Enr) -> u64 {
        self.local_metadata.read().custody_subnet_count()
            .unwrap_or_else(|| enr.custody_subnet_count())
    }

    /// Compute custody data columns the node is assigned to custody.
    pub fn custody_columns(&self) -> Vec<ColumnIndex> {
        let enr = self.local_enr();
        let node_id = Uint256::from(U256::from(enr.node_id().raw()));
        // TODO(das): cache this number at start-up to not make this fallible
        let custody_subnet_count = self.custody_subnet_count(enr);
        eip_7594::get_custody_columns(node_id, custody_subnet_count).collect()
    }

    /// Compute custody data column subnets the node is assigned to custody.
    pub fn custody_subnets(&self) -> impl Iterator<Item = SubnetId> {
        let enr = self.local_enr();
        let node_id = Uint256::from(U256::from(enr.node_id().raw()));
        let custody_subnet_count = self.custody_subnet_count(enr);
        eip_7594::get_custody_subnets(node_id, custody_subnet_count)
    }

    /// Returns a connected peer that:
    /// 1. is connected
    /// 2. assigned to custody the column based on it's `custody_subnet_count` from metadata (WIP)
    /// 3. has a good score
    /// 4. subscribed to the specified column - this condition can be removed later, so we can
    ///    identify and penalise peers that are supposed to custody the column.
    pub fn custody_peers_for_column(
        &self,
        column_index: ColumnIndex,
    ) -> Vec<PeerId> {
        self.peers
            .read()
            .good_custody_subnet_peer(misc::compute_subnet_for_data_column_sidecar(column_index))
            .cloned()
            .collect::<Vec<_>>()
    }

    /// TESTING ONLY. Build a dummy NetworkGlobals instance.
    pub fn new_test_globals(trusted_peers: Vec<PeerId>, log: &slog::Logger) -> NetworkGlobals {
        use crate::CombinedKeyExt;
        let keypair = libp2p::identity::secp256k1::Keypair::generate();
        let enr_key: discv5::enr::CombinedKey = discv5::enr::CombinedKey::from_secp256k1(&keypair);
        let enr = discv5::enr::Enr::builder().build(&enr_key).unwrap();
        NetworkGlobals::new(
            enr,
            MetaData::V3(MetaDataV3 {
                seq_number: 0,
                attnets: Default::default(),
                syncnets: Default::default(),
                custody_subnet_count: CUSTODY_REQUIREMENT,
            }),
            trusted_peers,
            false,
            log,
        )
    }
}

#[cfg(test)]
mod test {
    use slog::{o, Drain};
    use typenum::Unsigned as _;
    use types::eip7594::{NumberOfColumns, CUSTODY_REQUIREMENT, DATA_COLUMN_SIDECAR_SUBNET_COUNT};

    use super::*;

    pub fn build_log(level: slog::Level, enabled: bool) -> slog::Logger {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();

        if enabled {
            slog::Logger::root(drain.filter_level(level).fuse(), o!())
        } else {
            slog::Logger::root(drain.filter(|_| false).fuse(), o!())
        }
    }

    #[test]
    fn test_custody_count_default() {
        let log = build_log(slog::Level::Debug, false);
        let default_custody_requirement_column_count =
            NumberOfColumns::U64 / DATA_COLUMN_SIDECAR_SUBNET_COUNT * CUSTODY_REQUIREMENT;

        let globals = NetworkGlobals::new_test_globals(vec![], &log);
        let columns = globals.custody_columns();

        assert_eq!(
            columns.len(),
            default_custody_requirement_column_count as usize
        );
    }
}
