"""
Region validation module for seuron.

Provides functionality to check if compute clusters and storage buckets
are in the same GCP region to help identify potential latency and egress cost issues.
"""

import json
from typing import Dict, List, Optional, Set


def get_bucket_region(bucket_name: str) -> Optional[str]:
    """
    Get the GCP region for a given GCS bucket.

    Args:
        bucket_name: Name of the GCS bucket

    Returns:
        Region string (e.g., "US-CENTRAL1") or None if lookup fails
    """
    try:
        from google.cloud import storage
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        location = bucket.location
        # Location can be "US-CENTRAL1", "US", "EU", etc.
        return location.upper() if location else None
    except Exception as e:
        # Return None on any error (permissions, network, etc.)
        return None


def extract_region_from_zone(zone: str) -> str:
    """
    Extract region from GCP zone string.

    Args:
        zone: Zone string like "us-central1-a"

    Returns:
        Region string like "us-central1"
    """
    # Zone format: region-zone (e.g., "us-central1-a")
    # Split on last hyphen to get region
    parts = zone.rsplit('-', 1)
    return parts[0] if len(parts) > 1 else zone


def extract_bucket_regions(paths: List[str]) -> Dict[str, Optional[str]]:
    """
    Extract GCS bucket names from paths and get their regions.

    Args:
        paths: List of CloudVolume or GCS paths

    Returns:
        Dictionary mapping bucket name to region (or None if lookup failed)
    """
    from cloudfiles.paths import extract

    bucket_regions = {}
    for path in paths:
        if not path:
            continue
        try:
            components = extract(path)
            if components.protocol == "gs":
                bucket_name = components.bucket
                if bucket_name not in bucket_regions:
                    bucket_regions[bucket_name] = get_bucket_region(bucket_name)
        except Exception:
            # Skip paths that can't be parsed
            continue

    return bucket_regions


def normalize_region(region: str) -> str:
    """
    Normalize region string for comparison.

    Handles multi-region buckets (US, EU, ASIA) vs single-region (us-central1).

    Args:
        region: Region string from bucket or zone

    Returns:
        Normalized uppercase region string
    """
    return region.upper() if region else ""


def check_region_mismatch(
    bucket_regions: Dict[str, Optional[str]],
    compute_regions: Set[str]
) -> List[str]:
    """
    Check for region mismatches between storage and compute.

    Args:
        bucket_regions: Dict mapping bucket name to region
        compute_regions: Set of compute regions

    Returns:
        List of warning messages (empty if no mismatches)
    """
    warnings = []

    for bucket, bucket_region in bucket_regions.items():
        if bucket_region is None:
            warnings.append(f"Could not verify region for bucket '{bucket}' (may lack permissions)")
            continue

        # Normalize regions for comparison
        normalized_bucket_region = normalize_region(bucket_region)
        normalized_compute_regions = {normalize_region(r) for r in compute_regions}

        # Check if bucket region is multi-region (US, EU, ASIA)
        if normalized_bucket_region in ["US", "EU", "ASIA"]:
            # Multi-region buckets - check if compute is in same continent
            bucket_matches = False
            for compute_region in normalized_compute_regions:
                if normalized_bucket_region == "US" and compute_region.startswith("US-"):
                    bucket_matches = True
                    break
                elif normalized_bucket_region == "EU" and compute_region.startswith("EUROPE-"):
                    bucket_matches = True
                    break
                elif normalized_bucket_region == "ASIA" and compute_region.startswith("ASIA-"):
                    bucket_matches = True
                    break

            if not bucket_matches:
                warnings.append(
                    f"Bucket '{bucket}' is multi-region ({normalized_bucket_region}) "
                    f"but compute is in {', '.join(sorted(compute_regions))}"
                )
        else:
            # Single-region bucket - check exact match
            if normalized_bucket_region not in normalized_compute_regions:
                warnings.append(
                    f"Bucket '{bucket}' in {bucket_region.lower()} "
                    f"but cluster in {', '.join(sorted(compute_regions))}"
                )

    return warnings


def validate_compute_storage_regions(param: dict, cluster_key: str) -> None:
    """
    Validate that compute and storage regions match.

    Issues warnings via Slack if mismatches are detected, but does not
    raise exceptions or block execution.

    Args:
        param: DAG parameters containing paths
        cluster_key: Key for cluster in InstanceGroups connection (e.g., "gpu", "atomic")
    """
    from airflow.hooks.base_hook import BaseHook
    from slack_message import slack_message

    try:
        # Extract all GCS paths from parameters
        paths = [
            param.get("IMAGE_PATH"),
            param.get("OUTPUT_PATH"),
            param.get("IMAGE_MASK_PATH"),
            param.get("OUTPUT_MASK_PATH"),
        ]

        # Filter out None values
        paths = [p for p in paths if p]

        if not paths:
            # No GCS paths to check
            return

        # Get bucket regions
        bucket_regions = extract_bucket_regions(paths)

        if not bucket_regions:
            # No GCS buckets found
            return

        # Get cluster zones from InstanceGroups connection
        try:
            cluster_info = json.loads(BaseHook.get_connection("InstanceGroups").extra)
        except Exception:
            # No cluster info available (maybe not on GCP)
            return

        if cluster_key not in cluster_info:
            # Cluster key not found, skip validation
            return

        # Extract regions from cluster zones
        compute_regions = set()
        for instance_group in cluster_info[cluster_key]:
            zone = instance_group.get('zone')
            if zone:
                region = extract_region_from_zone(zone)
                compute_regions.add(region)

        if not compute_regions:
            # No compute regions found
            return

        # Check for mismatches
        warnings = check_region_mismatch(bucket_regions, compute_regions)

        if warnings:
            slack_message(":exclamation:*Warning:* Compute/storage region mismatch detected")
            for warning in warnings:
                slack_message(f"  • {warning}")
            slack_message("  • This may cause higher latency and additional egress charges")

    except Exception as e:
        # Catch-all to ensure validation errors don't break DAG execution
        # Log the error but don't raise
        from slack_message import slack_message
        slack_message(f":exclamation:*Warning:* Region validation failed: {str(e)}")


def validate_compute_storage_regions_wrapper(cluster_conn_id: str) -> None:
    """
    Wrapper function for use in segmentation DAGs.

    Retrieves parameters from Airflow Variable and calls validation.

    Args:
        cluster_conn_id: Cluster connection ID (e.g., "atomic", "composite")
    """
    from airflow.models import Variable

    try:
        param = Variable.get("param", deserialize_json=True)
        validate_compute_storage_regions(param, cluster_conn_id)
    except Exception as e:
        # Don't block execution on validation errors
        from slack_message import slack_message
        slack_message(f":exclamation:*Warning:* Region validation failed: {str(e)}")
