# sctys_rust_utilities

## Redis snapshot communication

Async latest-snapshot publishing, observation, and shared-worker leases are available
through `sctys_rust_utilities::redis`. See [the Redis guide](docs/redis.md) and
[the runnable example](examples/redis_snapshots.rs).

## Live integration test fixtures

Service tests require their existing credentials and logger configuration. Test
targets are supplied explicitly:

- `AWS_S3_TEST_FOLDER`: a provisioned S3 fixture folder for folder/file tests.
- `AWS_S3_TEST_FILE`: an existing file inside that folder for file tests.
- `CAPSOLVER_TEST_WEBSITE_URL` and `CAPSOLVER_TEST_SITE_KEY`: a test widget page
  and its site key for the Turnstile test.
- `CAPSOLVER_TEST_CHALLENGE_URL`: a challenge page for cookie-replay testing.

Pure unit and framework contract tests do not require these live fixtures.
