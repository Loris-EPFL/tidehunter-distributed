use std::{io, net::IpAddr};

use serde::{Deserialize, Serialize};

/// An explicitly selected libfabric provider profile.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FabricBackend {
    Efa,
    EfaDirect,
    Verbs,
    SoftwareTcp,
}

/// Per-endpoint TX and RX slot count used when `FabricConfig::slots` is `None`.
pub const DEFAULT_SLOTS: usize = 4;
/// Largest accepted per-endpoint slot count. Each slot is 64 KiB for TX and
/// for RX, so the bound caps one endpoint's registered buffers at 8 MiB.
pub const MAX_SLOTS: usize = 64;

/// Provider selection and host-local discovery constraints.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FabricConfig {
    pub backend: FabricBackend,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_address: Option<IpAddr>,
    /// TX and RX registered-slot count per endpoint, `1..=MAX_SLOTS`; `None`
    /// selects `DEFAULT_SLOTS`. EFA and EFA-direct queue geometry is validated
    /// only for the default count, so those backends accept `None` or the default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slots: Option<usize>,
}

impl FabricBackend {
    #[cfg(feature = "native")]
    pub(crate) const fn provider(self) -> &'static str {
        match self {
            Self::Efa | Self::EfaDirect => "efa",
            Self::Verbs => "verbs;ofi_rxm",
            Self::SoftwareTcp => "tcp;ofi_rxm",
        }
    }

    #[cfg(feature = "native")]
    pub(crate) const fn is_direct(self) -> bool {
        matches!(self, Self::EfaDirect)
    }
}

impl FabricConfig {
    /// Slot count after applying the default.
    #[must_use]
    pub fn effective_slots(&self) -> usize {
        self.slots.unwrap_or(DEFAULT_SLOTS)
    }

    /// Validate selectors without consulting the local provider installation.
    ///
    /// # Errors
    /// Returns invalid input for malformed selectors or source addresses not
    /// supported by the selected provider profile.
    pub fn validate(&self) -> io::Result<()> {
        if let Some(domain) = &self.domain
            && (domain.is_empty() || domain.len() > 255 || domain.as_bytes().contains(&0))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "fabric domain must contain 1..=255 non-NUL bytes",
            ));
        }
        if self.source_address.is_some_and(|address| match address {
            IpAddr::V4(address) => address.is_unspecified() || address.is_multicast(),
            IpAddr::V6(address) => address.is_unspecified() || address.is_multicast(),
        }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "fabric source address cannot be unspecified or multicast",
            ));
        }
        let slots = self.effective_slots();
        if !(1..=MAX_SLOTS).contains(&slots) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("fabric slots must be in 1..={MAX_SLOTS}"),
            ));
        }
        if matches!(self.backend, FabricBackend::Efa | FabricBackend::EfaDirect)
            && slots != DEFAULT_SLOTS
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "EFA queue geometry is validated only for {DEFAULT_SLOTS} slots; omit slots for EFA backends"
                ),
            ));
        }
        match (self.backend, self.source_address) {
            (FabricBackend::Efa | FabricBackend::EfaDirect, Some(_)) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "EFA source IP selection is unsupported for opaque FI_ADDR_EFA addresses",
            )),
            (FabricBackend::Verbs, Some(IpAddr::V6(_))) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "verbs;ofi_rxm source selection supports FI_SOCKADDR/FI_SOCKADDR_IN only",
            )),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_SLOTS, FabricBackend, FabricConfig, MAX_SLOTS};
    use std::{
        io,
        net::{IpAddr, Ipv4Addr},
    };

    #[test]
    fn invalid_domain_and_source_selectors_are_rejected() {
        let mut config = FabricConfig {
            backend: FabricBackend::Verbs,
            domain: Some(String::new()),
            source_address: None,
            slots: None,
        };
        assert_eq!(
            config.validate().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        config.domain = None;
        config.source_address = Some(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert_eq!(
            config.validate().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        config.source_address = Some(IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)));
        assert_eq!(
            config.validate().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        config.backend = FabricBackend::EfaDirect;
        config.source_address = Some(IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(
            config.validate().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        config.backend = FabricBackend::Verbs;
        config.source_address = Some(IpAddr::V6(std::net::Ipv6Addr::LOCALHOST));
        assert_eq!(
            config.validate().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn slot_count_is_bounded_and_efa_keeps_its_validated_geometry() {
        let config = |backend, slots| FabricConfig {
            backend,
            domain: None,
            source_address: None,
            slots,
        };
        assert_eq!(
            config(FabricBackend::Verbs, None).effective_slots(),
            DEFAULT_SLOTS
        );
        for slots in [1, DEFAULT_SLOTS, 16, MAX_SLOTS] {
            config(FabricBackend::Verbs, Some(slots))
                .validate()
                .unwrap();
            config(FabricBackend::SoftwareTcp, Some(slots))
                .validate()
                .unwrap();
        }
        for slots in [0, MAX_SLOTS + 1, usize::MAX] {
            assert_eq!(
                config(FabricBackend::Verbs, Some(slots))
                    .validate()
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
        }
        for backend in [FabricBackend::Efa, FabricBackend::EfaDirect] {
            config(backend, None).validate().unwrap();
            config(backend, Some(DEFAULT_SLOTS)).validate().unwrap();
            assert_eq!(
                config(backend, Some(8)).validate().unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        }
    }
}
