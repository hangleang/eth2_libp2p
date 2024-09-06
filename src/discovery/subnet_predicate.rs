//! The subnet predicate used for searching for a particular subnet.
use super::*;
use ethereum_types::U256;
use itertools::Itertools;
use slog::trace;
use ssz::Uint256;

/// Returns the predicate for a given subnet.
pub fn subnet_predicate(subnets: Vec<Subnet>, log: &slog::Logger) -> impl Fn(&Enr) -> bool + Send {
    let log_clone = log.clone();

    move |enr| {
        let Ok(attestation_bitfield) = enr.attestation_bitfield() else {
            return false;
        };

        // Pre-fork/fork-boundary enrs may not contain a syncnets field.
        // Don't return early here
        let sync_committee_bitfield = enr.sync_committee_bitfield().ok();

        let custody_subnet_count = enr.custody_subnet_count();

        let predicate = subnets.iter().copied().any(|subnet| match subnet {
            Subnet::Attestation(subnet_id) => attestation_bitfield
                .get(subnet_id as usize)
                .unwrap_or_default(),
            Subnet::SyncCommittee(subnet_id) => sync_committee_bitfield
                .and_then(|bitfield| bitfield.get(subnet_id as usize))
                .unwrap_or_default(),
            Subnet::DataColumn(s) => {
                let mut subnets = eip_7594::get_custody_columns(
                    Uint256::from(U256::from(enr.node_id().raw())),
                    custody_subnet_count,
                );
                trace!(
                    log_clone, 
                    "Here is the custody columns";
                    "custody_columns" => %subnets.join(", ")
                );
                subnets.contains(&s)
            }
        });

        if !predicate {
            trace!(
                log_clone,
                "Peer found but not on any of the desired subnets";
                "peer_id" => %enr.peer_id()
            );
        }
        predicate
    }
}
