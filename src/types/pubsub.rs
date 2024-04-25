//! Handles the encoding and decoding of pubsub messages.

use crate::types::{ForkContext, GossipEncoding, GossipKind, GossipTopic};
use crate::TopicHash;
use eip_7594::{DataColumnSidecar, DataColumnSubnetId};
use snap::raw::{decompress_len, Decoder, Encoder};
use ssz::{SszReadDefault, SszWrite as _, WriteError};
use std::boxed::Box;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use types::{
    altair::containers::{
        SignedBeaconBlock as AltairSignedBeaconBlock, SignedContributionAndProof,
        SyncCommitteeMessage,
    },
    bellatrix::containers::SignedBeaconBlock as BellatrixBeaconBlock,
    capella::containers::{SignedBeaconBlock as CapellaBeaconBlock, SignedBlsToExecutionChange},
    combined::{LightClientFinalityUpdate, LightClientOptimisticUpdate, SignedBeaconBlock},
    deneb::containers::{BlobSidecar, SignedBeaconBlock as DenebBeaconBlock},
    nonstandard::Phase,
    phase0::{
        containers::{
            Attestation, AttesterSlashing, ProposerSlashing, SignedAggregateAndProof,
            SignedBeaconBlock as Phase0SignedBeaconBlock, SignedVoluntaryExit,
        },
        primitives::{ForkDigest, SubnetId},
    },
    preset::Preset,
    traits::SignedBeaconBlock as _,
};

#[derive(Debug, Clone, PartialEq)]
pub enum PubsubMessage<P: Preset> {
    /// Gossipsub message providing notification of a new block.
    BeaconBlock(Arc<SignedBeaconBlock<P>>),
    /// Gossipsub message providing notification of a [`BlobSidecar`] along with the subnet id where it was received.
    BlobSidecar(Box<(SubnetId, Arc<BlobSidecar<P>>)>),
    /// Gossipsub message providing notification of a [`DataColumnSidecar`] along with the subnet id where it was received.
    DataColumnSidecar(Box<(DataColumnSubnetId, Arc<DataColumnSidecar<P>>)>),
    /// Gossipsub message providing notification of a Aggregate attestation and associated proof.
    AggregateAndProofAttestation(Box<SignedAggregateAndProof<P>>),
    /// Gossipsub message providing notification of a raw un-aggregated attestation with its shard id.
    Attestation(SubnetId, Arc<Attestation<P>>),
    /// Gossipsub message providing notification of a voluntary exit.
    VoluntaryExit(Box<SignedVoluntaryExit>),
    /// Gossipsub message providing notification of a new proposer slashing.
    ProposerSlashing(Box<ProposerSlashing>),
    /// Gossipsub message providing notification of a new attester slashing.
    AttesterSlashing(Box<AttesterSlashing<P>>),
    /// Gossipsub message providing notification of partially aggregated sync committee signatures.
    SignedContributionAndProof(Box<SignedContributionAndProof<P>>),
    /// Gossipsub message providing notification of unaggregated sync committee signatures with its subnet id.
    SyncCommitteeMessage(Box<(SubnetId, SyncCommitteeMessage)>),
    /// Gossipsub message for BLS to execution change messages.
    BlsToExecutionChange(Box<SignedBlsToExecutionChange>),
    /// Gossipsub message providing notification of a light client finality update.
    LightClientFinalityUpdate(Box<LightClientFinalityUpdate<P>>),
    /// Gossipsub message providing notification of a light client optimistic update.
    LightClientOptimisticUpdate(Box<LightClientOptimisticUpdate<P>>),
}

// Implements the `DataTransform` trait of gossipsub to employ snappy compression
pub struct SnappyTransform {
    /// Sets the maximum size we allow gossipsub messages to decompress to.
    max_size_per_message: usize,
}

impl SnappyTransform {
    pub fn new(max_size_per_message: usize) -> Self {
        SnappyTransform {
            max_size_per_message,
        }
    }
}

impl gossipsub::DataTransform for SnappyTransform {
    // Provides the snappy decompression from RawGossipsubMessages
    fn inbound_transform(
        &self,
        raw_message: gossipsub::RawMessage,
    ) -> Result<gossipsub::Message, std::io::Error> {
        // check the length of the raw bytes
        let len = decompress_len(&raw_message.data)?;
        if len > self.max_size_per_message {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "ssz_snappy decoded data > GOSSIP_MAX_SIZE",
            ));
        }

        let mut decoder = Decoder::new();
        let decompressed_data = decoder.decompress_vec(&raw_message.data)?;

        // Build the GossipsubMessage struct
        Ok(gossipsub::Message {
            source: raw_message.source,
            data: decompressed_data,
            sequence_number: raw_message.sequence_number,
            topic: raw_message.topic,
        })
    }

    /// Provides the snappy compression logic to gossipsub.
    fn outbound_transform(
        &self,
        _topic: &TopicHash,
        data: Vec<u8>,
    ) -> Result<Vec<u8>, std::io::Error> {
        // Currently we are not employing topic-based compression. Everything is expected to be
        // snappy compressed.
        if data.len() > self.max_size_per_message {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "ssz_snappy Encoded data > GOSSIP_MAX_SIZE",
            ));
        }
        let mut encoder = Encoder::new();
        encoder.compress_vec(&data).map_err(Into::into)
    }
}

impl<P: Preset> PubsubMessage<P> {
    /// Returns the topics that each pubsub message will be sent across, given a supported
    /// gossipsub encoding and fork version.
    pub fn topics(&self, encoding: GossipEncoding, fork_digest: ForkDigest) -> Vec<GossipTopic> {
        vec![GossipTopic::new(self.kind(), encoding, fork_digest)]
    }

    /// Returns the kind of gossipsub topic associated with the message.
    pub fn kind(&self) -> GossipKind {
        match self {
            PubsubMessage::BeaconBlock(_) => GossipKind::BeaconBlock,
            PubsubMessage::BlobSidecar(blob_sidecar_data) => {
                GossipKind::BlobSidecar(blob_sidecar_data.0)
            }
            PubsubMessage::DataColumnSidecar(column_sidecar_data) => {
                GossipKind::DataColumnSidecar(column_sidecar_data.0)
            }
            PubsubMessage::AggregateAndProofAttestation(_) => GossipKind::BeaconAggregateAndProof,
            PubsubMessage::Attestation(subnet_id, _) => GossipKind::Attestation(*subnet_id),
            PubsubMessage::VoluntaryExit(_) => GossipKind::VoluntaryExit,
            PubsubMessage::ProposerSlashing(_) => GossipKind::ProposerSlashing,
            PubsubMessage::AttesterSlashing(_) => GossipKind::AttesterSlashing,
            PubsubMessage::SignedContributionAndProof(_) => GossipKind::SignedContributionAndProof,
            PubsubMessage::SyncCommitteeMessage(data) => GossipKind::SyncCommitteeMessage(data.0),
            PubsubMessage::BlsToExecutionChange(_) => GossipKind::BlsToExecutionChange,
            PubsubMessage::LightClientFinalityUpdate(_) => GossipKind::LightClientFinalityUpdate,
            PubsubMessage::LightClientOptimisticUpdate(_) => {
                GossipKind::LightClientOptimisticUpdate
            }
        }
    }

    /// This decodes `data` into a `PubsubMessage` given a topic.
    /* Note: This is assuming we are not hashing topics. If we choose to hash topics, these will
     * need to be modified.
     */
    pub fn decode(
        topic: &TopicHash,
        data: &[u8],
        fork_context: &ForkContext,
    ) -> Result<Self, String> {
        match GossipTopic::decode(topic.as_str()) {
            Err(_) => Err(format!("Unknown gossipsub topic: {:?}", topic)),
            Ok(gossip_topic) => {
                // All topics are currently expected to be compressed and decompressed with snappy.
                // This is done in the `SnappyTransform` struct.
                // Therefore compression has already been handled for us by the time we are
                // decoding the objects here.

                // the ssz decoders
                match gossip_topic.kind() {
                    GossipKind::BeaconAggregateAndProof => {
                        let agg_and_proof = SignedAggregateAndProof::from_ssz_default(data)
                            .map_err(|e| format!("{:?}", e))?;
                        Ok(PubsubMessage::AggregateAndProofAttestation(Box::new(
                            agg_and_proof,
                        )))
                    }
                    GossipKind::Attestation(subnet_id) => {
                        let attestation =
                            Attestation::from_ssz_default(data).map_err(|e| format!("{:?}", e))?;
                        Ok(PubsubMessage::Attestation(
                            *subnet_id,
                            Arc::new(attestation),
                        ))
                    }
                    GossipKind::BeaconBlock => {
                        let beacon_block =
                            match fork_context.from_context_bytes(gossip_topic.fork_digest) {
                                Some(Phase::Phase0) => SignedBeaconBlock::Phase0(
                                    Phase0SignedBeaconBlock::from_ssz_default(data)
                                        .map_err(|e| format!("{:?}", e))?,
                                ),
                                Some(Phase::Altair) => SignedBeaconBlock::Altair(
                                    AltairSignedBeaconBlock::from_ssz_default(data)
                                        .map_err(|e| format!("{:?}", e))?,
                                ),
                                Some(Phase::Bellatrix) => SignedBeaconBlock::Bellatrix(
                                    BellatrixBeaconBlock::from_ssz_default(data)
                                        .map_err(|e| format!("{:?}", e))?,
                                ),
                                Some(Phase::Capella) => SignedBeaconBlock::Capella(
                                    CapellaBeaconBlock::from_ssz_default(data)
                                        .map_err(|e| format!("{:?}", e))?,
                                ),
                                Some(Phase::Deneb) => SignedBeaconBlock::Deneb(
                                    DenebBeaconBlock::from_ssz_default(data)
                                        .map_err(|e| format!("{:?}", e))?,
                                ),
                                None => {
                                    return Err(format!(
                                        "Unknown gossipsub fork digest: {:?}",
                                        gossip_topic.fork_digest
                                    ))
                                }
                            };
                        Ok(PubsubMessage::BeaconBlock(Arc::new(beacon_block)))
                    }
                    GossipKind::BlobSidecar(blob_index) => {
                        match fork_context.from_context_bytes(gossip_topic.fork_digest) {
                            Some(Phase::Deneb) => {
                                let blob_sidecar = Arc::new(
                                    BlobSidecar::from_ssz_default(data)
                                        .map_err(|e| format!("{:?}", e))?,
                                );
                                Ok(PubsubMessage::BlobSidecar(Box::new((
                                    *blob_index,
                                    blob_sidecar,
                                ))))
                            }
                            Some(
                                Phase::Phase0 | Phase::Altair | Phase::Bellatrix | Phase::Capella,
                            )
                            | None => Err(format!(
                                "beacon_blobs_and_sidecar topic invalid for given fork digest {:?}",
                                gossip_topic.fork_digest
                            )),
                        }
                    }
                    GossipKind::DataColumnSidecar(subnet_id) => {
                        match fork_context.from_context_bytes(gossip_topic.fork_digest) {
                            Some(Phase::Deneb) => {
                                let col_sidecar = Arc::new(
                                    DataColumnSidecar::from_ssz_default(data)
                                        .map_err(|e| format!("{:?}", e))?,
                                );
                                Ok(PubsubMessage::DataColumnSidecar(Box::new((
                                    *subnet_id,
                                    col_sidecar,
                                ))))
                            }
                            Some(
                                Phase::Phase0 | Phase::Altair | Phase::Bellatrix | Phase::Capella,
                            )
                            | None => Err(format!(
                                "data_column_sidecar topic invalid for given fork digest {:?}",
                                gossip_topic.fork_digest
                            )),
                        }
                    }
                    GossipKind::VoluntaryExit => {
                        let voluntary_exit = SignedVoluntaryExit::from_ssz_default(data)
                            .map_err(|e| format!("{:?}", e))?;
                        Ok(PubsubMessage::VoluntaryExit(Box::new(voluntary_exit)))
                    }
                    GossipKind::ProposerSlashing => {
                        let proposer_slashing = ProposerSlashing::from_ssz_default(data)
                            .map_err(|e| format!("{:?}", e))?;
                        Ok(PubsubMessage::ProposerSlashing(Box::new(proposer_slashing)))
                    }
                    GossipKind::AttesterSlashing => {
                        let attester_slashing = AttesterSlashing::from_ssz_default(data)
                            .map_err(|e| format!("{:?}", e))?;
                        Ok(PubsubMessage::AttesterSlashing(Box::new(attester_slashing)))
                    }
                    GossipKind::SignedContributionAndProof => {
                        let sync_aggregate = SignedContributionAndProof::from_ssz_default(data)
                            .map_err(|e| format!("{:?}", e))?;
                        Ok(PubsubMessage::SignedContributionAndProof(Box::new(
                            sync_aggregate,
                        )))
                    }
                    GossipKind::SyncCommitteeMessage(subnet_id) => {
                        let sync_committee = SyncCommitteeMessage::from_ssz_default(data)
                            .map_err(|e| format!("{:?}", e))?;
                        Ok(PubsubMessage::SyncCommitteeMessage(Box::new((
                            *subnet_id,
                            sync_committee,
                        ))))
                    }
                    GossipKind::BlsToExecutionChange => {
                        let bls_to_execution_change =
                            SignedBlsToExecutionChange::from_ssz_default(data)
                                .map_err(|e| format!("{:?}", e))?;
                        Ok(PubsubMessage::BlsToExecutionChange(Box::new(
                            bls_to_execution_change,
                        )))
                    }
                    GossipKind::LightClientFinalityUpdate => {
                        let light_client_finality_update =
                            match fork_context.from_context_bytes(gossip_topic.fork_digest) {
                                Some(Phase::Phase0) => {
                                    return Err(format!(
                                        "light_client_finality_update topic invalid for given fork digest {:?}",
                                        gossip_topic.fork_digest,
                                    ))
                                }
                                Some(Phase::Altair | Phase::Bellatrix) => {
                                    SszReadDefault::from_ssz_default(data)
                                        .map(LightClientFinalityUpdate::Altair)
                                        .map_err(|e| format!("{:?}", e))?
                                        .into()
                                }
                                Some(Phase::Capella) => {
                                    SszReadDefault::from_ssz_default(data)
                                        .map(LightClientFinalityUpdate::Capella)
                                        .map_err(|e| format!("{:?}", e))?
                                        .into()
                                }
                                Some(Phase::Deneb) => {
                                    SszReadDefault::from_ssz_default(data)
                                        .map(LightClientFinalityUpdate::Deneb)
                                        .map_err(|e| format!("{:?}", e))?
                                        .into()
                                }
                                None => {
                                    return Err(format!(
                                        "light_client_finality_update topic invalid for given fork digest {:?}",
                                        gossip_topic.fork_digest,
                                    ))
                                }
                            };

                        Ok(PubsubMessage::LightClientFinalityUpdate(Box::new(
                            light_client_finality_update,
                        )))
                    }
                    GossipKind::LightClientOptimisticUpdate => {
                        let light_client_optimistic_update =
                            match fork_context.from_context_bytes(gossip_topic.fork_digest) {
                                Some(Phase::Phase0) => {
                                    return Err(format!(
                                        "light_client_optimistic_update topic invalid for given fork digest {:?}",
                                        gossip_topic.fork_digest,
                                    ))
                                }
                                Some(Phase::Altair | Phase::Bellatrix) => {
                                    SszReadDefault::from_ssz_default(data)
                                        .map(LightClientOptimisticUpdate::Altair)
                                        .map_err(|e| format!("{:?}", e))?
                                }
                                Some(Phase::Capella) => {
                                    SszReadDefault::from_ssz_default(data)
                                        .map(LightClientOptimisticUpdate::Capella)
                                        .map_err(|e| format!("{:?}", e))?
                                }
                                Some(Phase::Deneb) => {
                                    SszReadDefault::from_ssz_default(data)
                                        .map(LightClientOptimisticUpdate::Deneb)
                                        .map_err(|e| format!("{:?}", e))?
                                }
                                None => {
                                    return Err(format!(
                                        "light_client_optimistic_update topic invalid for given fork digest {:?}",
                                        gossip_topic.fork_digest
                                    ))
                                }
                            };

                        Ok(PubsubMessage::LightClientOptimisticUpdate(Box::new(
                            light_client_optimistic_update,
                        )))
                    }
                }
            }
        }
    }

    /// Encodes a `PubsubMessage` based on the topic encodings. The first known encoding is used. If
    /// no encoding is known, and error is returned.
    pub fn encode(&self, _encoding: GossipEncoding) -> Result<Vec<u8>, WriteError> {
        // Currently do not employ encoding strategies based on the topic. All messages are ssz
        // encoded.
        // Also note, that the compression is handled by the `SnappyTransform` struct. Gossipsub will compress the
        // messages for us.
        match &self {
            PubsubMessage::BeaconBlock(data) => data.to_ssz(),
            PubsubMessage::BlobSidecar(data) => data.1.to_ssz(),
            PubsubMessage::DataColumnSidecar(data) => data.1.to_ssz(),
            PubsubMessage::AggregateAndProofAttestation(data) => data.to_ssz(),
            PubsubMessage::VoluntaryExit(data) => data.to_ssz(),
            PubsubMessage::ProposerSlashing(data) => data.to_ssz(),
            PubsubMessage::AttesterSlashing(data) => data.to_ssz(),
            PubsubMessage::Attestation(_, attestation) => attestation.to_ssz(),
            PubsubMessage::SignedContributionAndProof(data) => data.to_ssz(),
            PubsubMessage::SyncCommitteeMessage(data) => data.1.to_ssz(),
            PubsubMessage::BlsToExecutionChange(data) => data.to_ssz(),
            PubsubMessage::LightClientFinalityUpdate(data) => data.to_ssz(),
            PubsubMessage::LightClientOptimisticUpdate(data) => data.to_ssz(),
        }
    }
}

impl<P: Preset> std::fmt::Display for PubsubMessage<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PubsubMessage::BeaconBlock(block) => write!(
                f,
                "Beacon Block: slot: {}, proposer_index: {}",
                block.message().slot(),
                block.message().proposer_index()
            ),
            PubsubMessage::BlobSidecar(data) => write!(
                f,
                "BlobSidecar: slot: {}, blob index: {}",
                data.1.signed_block_header.message.slot, data.1.index,
            ),
            PubsubMessage::DataColumnSidecar(data) => write!(
                f,
                "DataColumnSidecar: slot: {}, column index: {}",
                data.1.slot(),
                data.1.index,
            ),
            PubsubMessage::AggregateAndProofAttestation(att) => write!(
                f,
                "Aggregate and Proof: slot: {}, index: {}, aggregator_index: {}",
                att.message.aggregate.data.slot,
                att.message.aggregate.data.index,
                att.message.aggregator_index,
            ),
            PubsubMessage::Attestation(subnet_id, attestation) => write!(
                f,
                "Attestation: subnet_id: {}, attestation_slot: {}, attestation_index: {}",
                subnet_id, attestation.data.slot, attestation.data.index,
            ),
            PubsubMessage::VoluntaryExit(_data) => write!(f, "Voluntary Exit"),
            PubsubMessage::ProposerSlashing(_data) => write!(f, "Proposer Slashing"),
            PubsubMessage::AttesterSlashing(_data) => write!(f, "Attester Slashing"),
            PubsubMessage::SignedContributionAndProof(_) => {
                write!(f, "Signed Contribution and Proof")
            }
            PubsubMessage::SyncCommitteeMessage(data) => {
                write!(f, "Sync committee message: subnet_id: {}", data.0)
            }
            PubsubMessage::BlsToExecutionChange(data) => {
                write!(
                    f,
                    "Signed BLS to execution change: validator_index: {}, address: {:?}",
                    data.message.validator_index, data.message.to_execution_address
                )
            }
            PubsubMessage::LightClientFinalityUpdate(_data) => {
                write!(f, "Light CLient Finality Update")
            }
            PubsubMessage::LightClientOptimisticUpdate(_data) => {
                write!(f, "Light CLient Optimistic Update")
            }
        }
    }
}
