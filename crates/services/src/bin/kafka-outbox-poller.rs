fn main() -> std::process::ExitCode {
    services::entrypoint(services::Role::OutboxPoller)
}
