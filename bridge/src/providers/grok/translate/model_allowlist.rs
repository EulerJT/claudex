pub fn resolve_model(model: &str) -> String {
    model.to_string()
}

pub fn assert_allowed_model(model: &str) -> anyhow::Result<()> {
    if matches!(model, "grok-composer-2.5-fast" | "grok-4.5") {
        Ok(())
    } else {
        anyhow::bail!("unsupported Grok model")
    }
}
