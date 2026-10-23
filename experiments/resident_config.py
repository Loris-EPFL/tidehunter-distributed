"""Canonical resident-read limits for source-bound local and cloud experiments."""

MIB = 1024 * 1024
MAX_REGISTERED_BYTES = 256 * MIB
MAX_SOURCE_REGIONS = 65_536
MAX_DESTINATION_SLOTS = 8_192
MAX_HANDLES = 65_536


def resident_read_config(value=None):
    fields = ('source_bytes', 'source_regions', 'destination_bytes', 'destination_slots',
              'max_read_bytes', 'max_handles')
    disabled = dict.fromkeys(fields, 0) | dict(require_device_read=False)
    if value is None:
        return disabled
    if (not isinstance(value, dict) or set(value) != set(disabled)
            or any(type(value[field]) is not int or value[field] < 0 for field in fields)
            or type(value['require_device_read']) is not bool
            or (value != disabled and any(value[field] == 0 for field in fields))):
        raise ValueError('resident_reads requires complete bounded source/destination/handle limits')
    if (value['source_bytes'] > MAX_REGISTERED_BYTES
            or value['destination_bytes'] > MAX_REGISTERED_BYTES
            or value['source_regions'] > MAX_SOURCE_REGIONS
            or value['destination_slots'] > MAX_DESTINATION_SLOTS
            or value['max_handles'] > MAX_HANDLES
            or (value['destination_slots'] and value['max_read_bytes'] >
                value['destination_bytes'] // value['destination_slots'])):
        raise ValueError('resident_reads exceeds the fixed registration pool limits')
    return dict(value)


def validate_destination_total(config, databases):
    """Each RemoteDatabase owns pools; all namespaces share one process limit."""
    if (config['destination_bytes'] * databases > MAX_REGISTERED_BYTES
            or config['destination_slots'] * databases > MAX_DESTINATION_SLOTS):
        raise ValueError('resident_reads aggregate destination budget exceeds process limits')
