use super::OptionKind;

pub(super) struct Config {
    pub(super) callers: Vec<usize>,
    pub(super) owners: Vec<usize>,
    pub(super) batches: Vec<usize>,
    pub(super) fractions: Vec<usize>,
    pub(super) distributions: Vec<bool>,
    pub(super) options: Vec<OptionKind>,
    pub(super) seconds: usize,
    pub(super) repeats: usize,
    pub(super) csv: String,
    pub(super) json: String,
    pub(super) label: String,
    pub(super) criterion: bool,
    pub(super) codec: bool,
    pub(super) sample_limit: usize,
}
impl Config {
    pub(super) fn parse() -> Self {
        let mut config = Self {
            callers: vec![1, 4, 16, 64, 256],
            owners: vec![16, 4, 1],
            batches: vec![2, 22, 57, 1],
            fractions: vec![55, 100, 0],
            distributions: vec![false, true],
            options: vec![OptionKind::A, OptionKind::B, OptionKind::C, OptionKind::D],
            seconds: 1,
            repeats: 1,
            csv: "admission-options.csv".into(),
            json: "admission-options.jsonl".into(),
            label: "laptop, development only: not decision data".into(),
            criterion: false,
            codec: true,
            sample_limit: super::MAX_SAMPLES,
        };
        let mut args = std::env::args().skip(1);
        while let Some(flag) = args.next() {
            match flag.as_str() {
                // Cargo's harness=false benchmark marker, not a workload option.
                "--bench" => continue,
                "--criterion" => {
                    config.criterion = true;
                    continue;
                }
                "--skip-codec" => {
                    config.codec = false;
                    continue;
                }
                "--help" | "-h" => {
                    println!(
                        "Admission-core models (no owner/network/storage I/O)\n\
                        --callers 1,4,16,64,256  --owners 1,4,16\n\
                        --batches 1,2,22,57      --fractions-pct 0,55,100\n\
                        --distributions uniform,zipf099 --options A-model,B,C,D\n\
                        --seconds 1 --repeats 1 --output results.csv\n\
                        --json-output results.jsonl (use - for JSONL stdout)\n\
                        --label 'laptop, development only: not decision data'\n\
                        --skip-codec --criterion\n\
                        --sample-limit 8000000 (exact samples per caller; fail, never truncate)\n\
                        Threads are pinned automatically; restrict allowed CPUs with taskset.\n\
                        A put is one whole batch. Positive multi-owner fraction is impossible\n\
                        with one key or one owner; those cells run once at f=0."
                    );
                    std::process::exit(0);
                }
                _ => {}
            }
            let value = args
                .next()
                .unwrap_or_else(|| panic!("missing value for {flag}; use --help"));
            match flag.as_str() {
                "--callers" => config.callers = numbers(&value),
                "--owners" => config.owners = numbers(&value),
                "--batches" => config.batches = numbers(&value),
                "--fractions-pct" => config.fractions = numbers(&value),
                "--seconds" => config.seconds = value.parse().expect("integer seconds"),
                "--repeats" => config.repeats = value.parse().expect("integer repetitions"),
                "--sample-limit" => {
                    config.sample_limit = value.parse().expect("integer sample limit")
                }
                "--output" => config.csv = value,
                "--json-output" => config.json = value,
                "--label" => config.label = value,
                "--distributions" => {
                    config.distributions = value
                        .split(',')
                        .map(|name| match name {
                            "uniform" => false,
                            "zipf099" => true,
                            _ => panic!("unknown distribution {name}"),
                        })
                        .collect()
                }
                "--options" => {
                    config.options = value
                        .split(',')
                        .map(|name| match name {
                            "A-model" => OptionKind::A,
                            "B" => OptionKind::B,
                            "C" => OptionKind::C,
                            "D" => OptionKind::D,
                            _ => panic!("unknown option {name}"),
                        })
                        .collect()
                }
                _ => panic!("unknown argument {flag}; use --help"),
            }
        }
        assert!(config.seconds > 0 && config.repeats > 0);
        assert!(config.callers.iter().all(|c| (1..=256).contains(c)));
        assert!(
            config
                .owners
                .iter()
                .all(|o| o.is_power_of_two() && *o <= 16)
        );
        assert!(config.batches.iter().all(|b| (1..=64).contains(b)));
        assert!(config.fractions.iter().all(|f| *f <= 100));
        assert!(config.sample_limit > 0);
        assert!(
            !config.label.contains(['\n', '\r']),
            "label must be a single line"
        );
        assert!(
            !config.criterion || config.json != "-",
            "Criterion writes human output; JSON stdout requires matrix mode"
        );
        config
    }
}
fn numbers(value: &str) -> Vec<usize> {
    let result: Vec<_> = value
        .split(',')
        .map(|v| v.parse().expect("comma-separated integer parameter"))
        .collect();
    assert!(!result.is_empty());
    result
}
