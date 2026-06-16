pub struct FeatureCollectionDataset {
    pub url: String,
    pub asset_name: String,
}

impl FeatureCollectionDataset {
    pub fn open(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        // Simple extraction: assume URL might have asset name as fragment or parameter
        // For minimal scaffolding, just store the url.
        Ok(Self {
            url: url.to_string(),
            asset_name: "swir22".to_string(), // hardcoded for scaffolding
        })
    }
}

pub fn build_stac_url(
    base_url: &str,
    constraints: &crate::query_planner::QueryConstraints,
) -> Result<String, String> {
    let mut url = base_url.to_string();

    let lat_bounds = constraints
        .bounds
        .get("lat")
        .copied()
        .unwrap_or((None, None));
    let lon_bounds = constraints
        .bounds
        .get("lon")
        .copied()
        .unwrap_or((None, None));

    if let (Some(lon_min), Some(lat_min), Some(lon_max), Some(lat_max)) =
        (lon_bounds.0, lat_bounds.0, lon_bounds.1, lat_bounds.1)
    {
        let separator = if url.contains('?') { "&" } else { "?" };
        url = format!(
            "{}{separator}bbox={},{},{},{}",
            url, lon_min, lat_min, lon_max, lat_max
        );
    }

    let time_bounds = constraints
        .bounds
        .get("time")
        .copied()
        .unwrap_or((None, None));

    let separator = if url.contains('?') { "&" } else { "?" };
    match (time_bounds.0, time_bounds.1) {
        (None, None) => {}
        (Some(start), None) => {
            let start_str = crate::datetime::epoch_seconds_to_rfc3339(start)?.replace('+', "%2B");
            url = format!("{}{separator}datetime={start_str}/..", url);
        }
        (None, Some(end)) => {
            let end_str = crate::datetime::epoch_seconds_to_rfc3339(end)?.replace('+', "%2B");
            url = format!("{}{separator}datetime=../{end_str}", url);
        }
        (Some(start), Some(end)) => {
            let start_str = crate::datetime::epoch_seconds_to_rfc3339(start)?.replace('+', "%2B");
            let end_str = crate::datetime::epoch_seconds_to_rfc3339(end)?.replace('+', "%2B");
            url = format!("{}{separator}datetime={start_str}/{end_str}", url);
        }
    }

    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_feature_collection() {
        let ds = FeatureCollectionDataset::open("https://example.com/stac").unwrap();
        assert_eq!(ds.url, "https://example.com/stac");
    }

    #[test]
    fn test_stac_filter_pushdown() {
        let mut bounds = std::collections::HashMap::new();
        bounds.insert("lat".to_string(), (Some(40.0), Some(45.0)));
        bounds.insert("lon".to_string(), (Some(-10.0), Some(10.0)));
        let constraints = crate::query_planner::QueryConstraints {
            bounds,
            pins: std::collections::HashMap::new(),
        };

        let url =
            crate::feature_collection::build_stac_url("https://example.com/search", &constraints).unwrap();
        assert!(url.contains("bbox=-10,40,10,45"));
    }

    #[test]
    fn test_stac_time_pushdown_closed() {
        let mut bounds = std::collections::HashMap::new();
        bounds.insert("time".to_string(), (Some(1767225600.0), Some(1798761600.0))); // Jan 1 2026 to Jan 1 2027
        let constraints = crate::query_planner::QueryConstraints {
            bounds,
            pins: std::collections::HashMap::new(),
        };

        let url =
            crate::feature_collection::build_stac_url("https://example.com/search", &constraints)
                .unwrap();
        assert!(url.contains("datetime=2026-01-01T00:00:00%2B00:00/2027-01-01T00:00:00%2B00:00"));
    }

    #[test]
    fn test_stac_time_pushdown_open_end() {
        let mut bounds = std::collections::HashMap::new();
        bounds.insert("time".to_string(), (Some(1767225600.0), None));
        let constraints = crate::query_planner::QueryConstraints {
            bounds,
            pins: std::collections::HashMap::new(),
        };

        let url =
            crate::feature_collection::build_stac_url("https://example.com/search", &constraints)
                .unwrap();
        assert!(url.contains("datetime=2026-01-01T00:00:00%2B00:00/.."));
    }

    #[test]
    fn test_stac_time_pushdown_open_start() {
        let mut bounds = std::collections::HashMap::new();
        bounds.insert("time".to_string(), (None, Some(1767225600.0)));
        let constraints = crate::query_planner::QueryConstraints {
            bounds,
            pins: std::collections::HashMap::new(),
        };

        let url =
            crate::feature_collection::build_stac_url("https://example.com/search", &constraints)
                .unwrap();
        assert!(url.contains("datetime=../2026-01-01T00:00:00%2B00:00"));
    }
}
