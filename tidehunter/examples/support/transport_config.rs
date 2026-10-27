use std::error::Error;
use std::path::Path;
use tidehunter::distributed::tcp::Transport;

pub fn load(path: &Path) -> Result<Transport, Box<dyn Error>> {
    let transport: Transport = serde_json::from_slice(&std::fs::read(path)?)?;
    transport.validate()?;
    Ok(transport)
}
