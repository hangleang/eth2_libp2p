use std::collections::HashSet;

use gossipsub::{IdentTopic as Topic, TopicHash};
use serde::{Deserialize, Serialize};
use strum::AsRefStr;
use typenum::Unsigned;
use types::{
    altair::consts::SyncCommitteeSubnetCount,
    config::Config as ChainConfig,
    nonstandard::Phase,
    phase0::{
        consts::AttestationSubnetCount,
        primitives::{ForkDigest, SubnetId},
    },
};

use crate::Subnet;

/// The gossipsub topic names.
// These constants form a topic name of the form /TOPIC_PREFIX/TOPIC/ENCODING_POSTFIX
// For example /eth2/beacon_block/ssz
pub const TOPIC_PREFIX: &str = "eth2";
pub const SSZ_SNAPPY_ENCODING_POSTFIX: &str = "ssz_snappy";
pub const BEACON_BLOCK_TOPIC: &str = "beacon_block";
pub const BEACON_AGGREGATE_AND_PROOF_TOPIC: &str = "beacon_aggregate_and_proof";
pub const BEACON_ATTESTATION_PREFIX: &str = "beacon_attestation_";
pub const BLOB_SIDECAR_PREFIX: &str = "blob_sidecar_";
pub const DATA_COLUMN_SIDECAR_PREFIX: &str = "data_column_sidecar_";
pub const VOLUNTARY_EXIT_TOPIC: &str = "voluntary_exit";
pub const PROPOSER_SLASHING_TOPIC: &str = "proposer_slashing";
pub const ATTESTER_SLASHING_TOPIC: &str = "attester_slashing";
pub const SIGNED_CONTRIBUTION_AND_PROOF_TOPIC: &str = "sync_committee_contribution_and_proof";
pub const SYNC_COMMITTEE_PREFIX_TOPIC: &str = "sync_committee_";
pub const BLS_TO_EXECUTION_CHANGE_TOPIC: &str = "bls_to_execution_change";
pub const LIGHT_CLIENT_FINALITY_UPDATE: &str = "light_client_finality_update";
pub const LIGHT_CLIENT_OPTIMISTIC_UPDATE: &str = "light_client_optimistic_update";

#[derive(Debug)]
pub struct TopicConfig<'a> {
    pub enable_light_client_server: bool,
    pub subscribe_all_subnets: bool,
    pub subscribe_all_data_column_subnets: bool,
    pub sampling_subnets: &'a HashSet<SubnetId>,
}

/// Returns all the topics the node should subscribe at `current_phase`
pub fn core_topics_to_subscribe(
    chain_config: &ChainConfig,
    current_phase: Phase,
    opts: &TopicConfig,
) -> Vec<GossipKind> {
    let mut topics = vec![
        GossipKind::BeaconBlock,
        GossipKind::BeaconAggregateAndProof,
        GossipKind::VoluntaryExit,
        GossipKind::ProposerSlashing,
        GossipKind::AttesterSlashing,
    ];

    if opts.subscribe_all_subnets {
        for i in 0..AttestationSubnetCount::U64 {
            topics.push(GossipKind::Attestation(i.into()));
        }
    }

    if current_phase >= Phase::Altair {
        topics.push(GossipKind::SignedContributionAndProof);

        if opts.subscribe_all_subnets {
            for i in 0..SyncCommitteeSubnetCount::U64 {
                topics.push(GossipKind::SyncCommitteeMessage(i.into()));
            }
        }

        if opts.enable_light_client_server {
            topics.push(GossipKind::LightClientFinalityUpdate);
            topics.push(GossipKind::LightClientOptimisticUpdate);
        }
    }

    if current_phase >= Phase::Capella {
        topics.push(GossipKind::BlsToExecutionChange);
    }

    if current_phase >= Phase::Deneb && !current_phase.is_peerdas_activated() {
        // All of deneb blob topics are core topics
        for i in 0..chain_config.blob_sidecar_subnet_count(current_phase).into() {
            topics.push(GossipKind::BlobSidecar(i));
        }
    }

    if current_phase.is_peerdas_activated() {
        if opts.subscribe_all_data_column_subnets {
            for column_subnet in 0..chain_config.data_column_sidecar_subnet_count {
                topics.push(GossipKind::DataColumnSidecar(column_subnet as SubnetId));
            }
        } else {
            for column_subnet in opts.sampling_subnets {
                topics.push(GossipKind::DataColumnSidecar(*column_subnet));
            }
        }
    }

    topics
}

/// Returns true if a given non-core `GossipTopic` MAY be subscribe at this fork.
///
/// For example: the `Attestation` topic is not subscribed as a core topic if
/// subscribe_all_subnets = false` but we may subscribe to it outside of a fork
/// boundary if the node is an aggregator.
pub fn is_fork_non_core_topic(topic: &GossipTopic, _phase: Phase) -> bool {
    match topic.kind() {
        // Node may be aggregator of attestation and sync_committee_message topics for all known
        // forks
        GossipKind::Attestation(_) | GossipKind::SyncCommitteeMessage(_) => true,
        // All these topics are core-only
        GossipKind::BeaconBlock
        | GossipKind::BeaconAggregateAndProof
        | GossipKind::BlobSidecar(_)
        | GossipKind::DataColumnSidecar(_)
        | GossipKind::VoluntaryExit
        | GossipKind::ProposerSlashing
        | GossipKind::AttesterSlashing
        | GossipKind::SignedContributionAndProof
        | GossipKind::BlsToExecutionChange
        | GossipKind::LightClientFinalityUpdate
        | GossipKind::LightClientOptimisticUpdate => false,
    }
}

pub fn all_topics_at_fork(chain_config: &ChainConfig, current_phase: Phase) -> Vec<GossipKind> {
    // Compute the worst case of all forks
    let sampling_subnets = HashSet::from_iter(0..chain_config.data_column_sidecar_subnet_count);
    let opts = TopicConfig {
        enable_light_client_server: true,
        subscribe_all_subnets: true,
        subscribe_all_data_column_subnets: true,
        sampling_subnets: &sampling_subnets,
    };
    core_topics_to_subscribe(chain_config, current_phase, &opts)
}

/// A gossipsub topic which encapsulates the type of messages that should be sent and received over
/// the pubsub protocol and the way the messages should be encoded.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct GossipTopic {
    /// The encoding of the topic.
    encoding: GossipEncoding,
    /// The fork digest of the topic,
    pub fork_digest: ForkDigest,
    /// The kind of topic.
    kind: GossipKind,
}

/// Enum that brings these topics into the rust type system.
// NOTE: There is intentionally no unknown type here. We only allow known gossipsub topics.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum GossipKind {
    /// Topic for publishing beacon blocks.
    BeaconBlock,
    /// Topic for publishing aggregate attestations and proofs.
    BeaconAggregateAndProof,
    /// Topic for publishing BlobSidecars.
    BlobSidecar(u64),
    /// Topic for publishing DataColumnSidecars.
    DataColumnSidecar(SubnetId),
    /// Topic for publishing raw attestations on a particular subnet.
    #[strum(serialize = "beacon_attestation")]
    Attestation(SubnetId),
    /// Topic for publishing voluntary exits.
    VoluntaryExit,
    /// Topic for publishing block proposer slashings.
    ProposerSlashing,
    /// Topic for publishing attester slashings.
    AttesterSlashing,
    /// Topic for publishing partially aggregated sync committee signatures.
    SignedContributionAndProof,
    /// Topic for publishing unaggregated sync committee signatures on a particular subnet.
    #[strum(serialize = "sync_committee")]
    SyncCommitteeMessage(SubnetId),
    /// Topic for validator messages which change their withdrawal address.
    BlsToExecutionChange,
    /// Topic for publishing finality updates for light clients.
    LightClientFinalityUpdate,
    /// Topic for publishing optimistic updates for light clients.
    LightClientOptimisticUpdate,
}

impl std::fmt::Display for GossipKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GossipKind::Attestation(subnet_id) => write!(f, "beacon_attestation_{}", *subnet_id),
            GossipKind::SyncCommitteeMessage(subnet_id) => {
                write!(f, "sync_committee_{}", *subnet_id)
            }
            GossipKind::BlobSidecar(blob_index) => {
                write!(f, "{}{}", BLOB_SIDECAR_PREFIX, blob_index)
            }
            GossipKind::DataColumnSidecar(column_index) => {
                write!(f, "{}{}", DATA_COLUMN_SIDECAR_PREFIX, column_index)
            }
            x => f.write_str(x.as_ref()),
        }
    }
}

/// The known encoding types for gossipsub messages.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
pub enum GossipEncoding {
    /// Messages are encoded with SSZSnappy.
    #[default]
    SSZSnappy,
}

impl GossipTopic {
    pub fn new(kind: GossipKind, encoding: GossipEncoding, fork_digest: ForkDigest) -> Self {
        GossipTopic {
            encoding,
            fork_digest,
            kind,
        }
    }

    /// Returns the encoding type for the gossipsub topic.
    pub fn encoding(&self) -> &GossipEncoding {
        &self.encoding
    }

    /// Returns a mutable reference to the fork digest of the gossipsub topic.
    pub fn digest(&mut self) -> &mut ForkDigest {
        &mut self.fork_digest
    }

    /// Returns the kind of message expected on the gossipsub topic.
    pub fn kind(&self) -> &GossipKind {
        &self.kind
    }

    pub fn decode(topic: &str) -> Result<Self, String> {
        let topic_parts: Vec<&str> = topic.split('/').collect();
        if topic_parts.len() == 5 && topic_parts[1] == TOPIC_PREFIX {
            let digest_bytes = hex::decode(topic_parts[2])
                .map_err(|e| format!("Could not decode fork_digest hex: {}", e))?;

            if digest_bytes.len() != 4 {
                return Err(format!(
                    "Invalid gossipsub fork digest size: {}",
                    digest_bytes.len()
                ));
            }

            let fork_digest = ForkDigest::from_slice(digest_bytes.as_slice());

            let encoding = match topic_parts[4] {
                SSZ_SNAPPY_ENCODING_POSTFIX => GossipEncoding::SSZSnappy,
                _ => return Err(format!("Unknown encoding: {}", topic)),
            };
            let kind = match topic_parts[3] {
                BEACON_BLOCK_TOPIC => GossipKind::BeaconBlock,
                BEACON_AGGREGATE_AND_PROOF_TOPIC => GossipKind::BeaconAggregateAndProof,
                SIGNED_CONTRIBUTION_AND_PROOF_TOPIC => GossipKind::SignedContributionAndProof,
                VOLUNTARY_EXIT_TOPIC => GossipKind::VoluntaryExit,
                PROPOSER_SLASHING_TOPIC => GossipKind::ProposerSlashing,
                ATTESTER_SLASHING_TOPIC => GossipKind::AttesterSlashing,
                BLS_TO_EXECUTION_CHANGE_TOPIC => GossipKind::BlsToExecutionChange,
                LIGHT_CLIENT_FINALITY_UPDATE => GossipKind::LightClientFinalityUpdate,
                LIGHT_CLIENT_OPTIMISTIC_UPDATE => GossipKind::LightClientOptimisticUpdate,
                topic => match subnet_topic_index(topic) {
                    Some(kind) => kind,
                    None => return Err(format!("Unknown topic: {}", topic)),
                },
            };

            return Ok(GossipTopic {
                encoding,
                fork_digest,
                kind,
            });
        }

        Err(format!("Unknown topic: {}", topic))
    }

    pub fn subnet_id(&self) -> Option<Subnet> {
        match self.kind() {
            GossipKind::Attestation(subnet_id) => Some(Subnet::Attestation(*subnet_id)),
            GossipKind::SyncCommitteeMessage(subnet_id) => Some(Subnet::SyncCommittee(*subnet_id)),
            GossipKind::DataColumnSidecar(subnet_id) => Some(Subnet::DataColumn(*subnet_id)),
            _ => None,
        }
    }
}

impl From<GossipTopic> for Topic {
    fn from(topic: GossipTopic) -> Topic {
        Topic::new(topic)
    }
}

impl From<GossipTopic> for String {
    fn from(topic: GossipTopic) -> String {
        // Use the `Display` implementation below.
        topic.to_string()
    }
}

impl std::fmt::Display for GossipTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let encoding = match self.encoding {
            GossipEncoding::SSZSnappy => SSZ_SNAPPY_ENCODING_POSTFIX,
        };

        let kind = match self.kind {
            GossipKind::BeaconBlock => BEACON_BLOCK_TOPIC.into(),
            GossipKind::BeaconAggregateAndProof => BEACON_AGGREGATE_AND_PROOF_TOPIC.into(),
            GossipKind::VoluntaryExit => VOLUNTARY_EXIT_TOPIC.into(),
            GossipKind::ProposerSlashing => PROPOSER_SLASHING_TOPIC.into(),
            GossipKind::AttesterSlashing => ATTESTER_SLASHING_TOPIC.into(),
            GossipKind::Attestation(index) => format!("{}{}", BEACON_ATTESTATION_PREFIX, index),
            GossipKind::SignedContributionAndProof => SIGNED_CONTRIBUTION_AND_PROOF_TOPIC.into(),
            GossipKind::SyncCommitteeMessage(index) => {
                format!("{}{}", SYNC_COMMITTEE_PREFIX_TOPIC, index)
            }
            GossipKind::BlobSidecar(blob_index) => {
                format!("{}{}", BLOB_SIDECAR_PREFIX, blob_index)
            }
            GossipKind::DataColumnSidecar(index) => {
                format!("{}{}", DATA_COLUMN_SIDECAR_PREFIX, index)
            }
            GossipKind::BlsToExecutionChange => BLS_TO_EXECUTION_CHANGE_TOPIC.into(),
            GossipKind::LightClientFinalityUpdate => LIGHT_CLIENT_FINALITY_UPDATE.into(),
            GossipKind::LightClientOptimisticUpdate => LIGHT_CLIENT_OPTIMISTIC_UPDATE.into(),
        };
        write!(
            f,
            "/{}/{}/{}/{}",
            TOPIC_PREFIX,
            hex::encode(self.fork_digest),
            kind,
            encoding
        )
    }
}

impl From<Subnet> for GossipKind {
    fn from(subnet_id: Subnet) -> Self {
        match subnet_id {
            Subnet::Attestation(s) => GossipKind::Attestation(s),
            Subnet::SyncCommittee(s) => GossipKind::SyncCommitteeMessage(s),
            Subnet::DataColumn(s) => GossipKind::DataColumnSidecar(s),
        }
    }
}

// helper functions

/// Get subnet id from an attestation subnet topic hash.
pub fn subnet_from_topic_hash(topic_hash: &TopicHash) -> Option<Subnet> {
    GossipTopic::decode(topic_hash.as_str()).ok()?.subnet_id()
}

// Determines if the topic name is of an indexed topic.
fn subnet_topic_index(topic: &str) -> Option<GossipKind> {
    if let Some(index) = topic.strip_prefix(BEACON_ATTESTATION_PREFIX) {
        return Some(GossipKind::Attestation(index.parse::<SubnetId>().ok()?));
    } else if let Some(index) = topic.strip_prefix(SYNC_COMMITTEE_PREFIX_TOPIC) {
        return Some(GossipKind::SyncCommitteeMessage(
            index.parse::<SubnetId>().ok()?,
        ));
    } else if let Some(index) = topic.strip_prefix(BLOB_SIDECAR_PREFIX) {
        return Some(GossipKind::BlobSidecar(index.parse::<u64>().ok()?));
    } else if let Some(index) = topic.strip_prefix(DATA_COLUMN_SIDECAR_PREFIX) {
        return Some(GossipKind::DataColumnSidecar(index.parse::<u64>().ok()?));
    }
    None
}

#[cfg(test)]
mod tests {
    use enum_iterator::Sequence;
    use types::phase0::primitives::H32;

    use super::GossipKind::*;
    use super::*;

    const GOOD_FORK_DIGEST: &str = "e1925f3b";
    const BAD_PREFIX: &str = "tezos";
    const BAD_FORK_DIGEST: &str = "e1925f3b4b";
    const BAD_ENCODING: &str = "rlp";
    const BAD_KIND: &str = "blocks";

    fn topics() -> Vec<String> {
        let mut topics = Vec::new();
        let fork_digest: ForkDigest = H32([1, 2, 3, 4]);
        for encoding in [GossipEncoding::SSZSnappy].iter() {
            for kind in [
                BeaconBlock,
                BeaconAggregateAndProof,
                SignedContributionAndProof,
                Attestation(42),
                SyncCommitteeMessage(42),
                VoluntaryExit,
                ProposerSlashing,
                AttesterSlashing,
            ]
            .iter()
            {
                topics.push(GossipTopic::new(kind.clone(), encoding.clone(), fork_digest).into());
            }
        }
        topics
    }

    fn create_topic(prefix: &str, fork_digest: &str, kind: &str, encoding: &str) -> String {
        format!("/{}/{}/{}/{}", prefix, fork_digest, kind, encoding)
    }

    #[test]
    fn test_decode() {
        for topic in topics().iter() {
            assert!(GossipTopic::decode(topic.as_str()).is_ok());
        }
    }

    #[test]
    fn test_decode_malicious() {
        let bad_prefix_str = create_topic(
            BAD_PREFIX,
            GOOD_FORK_DIGEST,
            BEACON_BLOCK_TOPIC,
            SSZ_SNAPPY_ENCODING_POSTFIX,
        );
        assert!(GossipTopic::decode(bad_prefix_str.as_str()).is_err());

        let bad_digest_str = create_topic(
            TOPIC_PREFIX,
            BAD_FORK_DIGEST,
            BEACON_BLOCK_TOPIC,
            SSZ_SNAPPY_ENCODING_POSTFIX,
        );
        assert!(GossipTopic::decode(bad_digest_str.as_str()).is_err());

        let bad_kind_str = create_topic(
            TOPIC_PREFIX,
            GOOD_FORK_DIGEST,
            BAD_KIND,
            SSZ_SNAPPY_ENCODING_POSTFIX,
        );
        assert!(GossipTopic::decode(bad_kind_str.as_str()).is_err());

        let bad_encoding_str = create_topic(
            TOPIC_PREFIX,
            GOOD_FORK_DIGEST,
            BEACON_BLOCK_TOPIC,
            BAD_ENCODING,
        );
        assert!(GossipTopic::decode(bad_encoding_str.as_str()).is_err());

        // Extra parts
        assert!(
            GossipTopic::decode("/eth2/e1925f3b/beacon_block/ssz_snappy/yolo").is_err(),
            "should have exactly 5 parts"
        );
        // Empty string
        assert!(GossipTopic::decode("").is_err());
        // Empty parts
        assert!(GossipTopic::decode("////").is_err());
    }

    #[test]
    fn test_subnet_from_topic_hash() {
        let topic_hash = TopicHash::from_raw("/eth2/e1925f3b/beacon_block/ssz_snappy");
        assert!(subnet_from_topic_hash(&topic_hash).is_none());

        let topic_hash = TopicHash::from_raw("/eth2/e1925f3b/beacon_attestation_42/ssz_snappy");
        assert_eq!(
            subnet_from_topic_hash(&topic_hash),
            Some(Subnet::Attestation(42))
        );

        let topic_hash = TopicHash::from_raw("/eth2/e1925f3b/sync_committee_42/ssz_snappy");
        assert_eq!(
            subnet_from_topic_hash(&topic_hash),
            Some(Subnet::SyncCommittee(42))
        );
    }

    #[test]
    fn test_as_str_ref() {
        assert_eq!("beacon_block", BeaconBlock.as_ref());
        assert_eq!(
            "beacon_aggregate_and_proof",
            BeaconAggregateAndProof.as_ref()
        );
        assert_eq!("beacon_attestation", Attestation(42).as_ref());

        assert_eq!("sync_committee", SyncCommitteeMessage(42).as_ref());
        assert_eq!("voluntary_exit", VoluntaryExit.as_ref());
        assert_eq!("proposer_slashing", ProposerSlashing.as_ref());
        assert_eq!("attester_slashing", AttesterSlashing.as_ref());
    }

    fn get_chain_config() -> ChainConfig {
        let mut config = ChainConfig::default();
        config.altair_fork_epoch = 1;
        config.bellatrix_fork_epoch = 2;
        config.capella_fork_epoch = 3;
        config.deneb_fork_epoch = 4;
        config.electra_fork_epoch = 5;
        config.fulu_fork_epoch = 6;
        config
    }

    fn get_sampling_subnets() -> HashSet<SubnetId> {
        HashSet::new()
    }

    fn get_topic_config(sampling_subnets: &HashSet<SubnetId>) -> TopicConfig {
        TopicConfig {
            enable_light_client_server: false,
            subscribe_all_subnets: false,
            subscribe_all_data_column_subnets: false,
            sampling_subnets,
        }
    }

    #[test]
    fn base_topics_are_always_active() {
        let config = get_chain_config();
        let s = get_sampling_subnets();
        let topic_config = get_topic_config(&s);
        for phase in enum_iterator::all() {
            assert!(core_topics_to_subscribe(&config, phase, &topic_config)
                .contains(&GossipKind::BeaconBlock));
        }
    }

    #[test]
    fn blobs_are_not_subscribed_in_peerdas() {
        let config = get_chain_config();
        let s = get_sampling_subnets();
        let topic_config = get_topic_config(&s);
        assert!(
            !core_topics_to_subscribe(&config, Phase::Fulu, &topic_config)
                .contains(&GossipKind::BlobSidecar(0))
        );
    }

    #[test]
    fn columns_are_subscribed_in_peerdas() {
        let config = get_chain_config();
        let s = get_sampling_subnets();
        let mut topic_config = get_topic_config(&s);
        topic_config.subscribe_all_data_column_subnets = true;
        assert!(
            core_topics_to_subscribe(&config, Phase::Fulu, &topic_config)
                .contains(&GossipKind::DataColumnSidecar(0 as SubnetId))
        )
    }

    #[test]
    fn test_core_topics_to_subscribe() {
        let config = get_chain_config();
        let s = HashSet::from_iter([1, 2].map(|s| s as SubnetId));
        let mut topic_config = get_topic_config(&s);
        topic_config.enable_light_client_server = true;
        let latest_fork = Phase::last().unwrap_or(Phase::Phase0);
        let topics = core_topics_to_subscribe(&config, latest_fork, &topic_config);

        let mut expected_topics = vec![
            GossipKind::BeaconBlock,
            GossipKind::BeaconAggregateAndProof,
            GossipKind::VoluntaryExit,
            GossipKind::ProposerSlashing,
            GossipKind::AttesterSlashing,
            GossipKind::SignedContributionAndProof,
            GossipKind::LightClientFinalityUpdate,
            GossipKind::LightClientOptimisticUpdate,
            GossipKind::BlsToExecutionChange,
        ];
        for subnet in s {
            expected_topics.push(GossipKind::DataColumnSidecar(subnet));
        }
        // Need to check all the topics exist in an order independent manner
        for expected_topic in expected_topics {
            assert!(
                topics.contains(&expected_topic),
                "Should contain {:?}",
                expected_topic,
            );
        }
    }
}
