# Contributing to Dynomite

This is an unofficial fork of [Netflix/dynomite](https://github.com/Netflix/dynomite), see
[README.md](README.md). Bug reports and pull requests for this fork belong here, not upstream.

## Reporting bugs

Please open an [issue](https://github.com/zcalusic/dynomite/issues) and include:

- the Dynomite version (`dynomite -V`) and how it was built,
- the configuration file, with any secrets removed,
- the datastore (Redis or Memcached) and its version,
- what you did, what you expected, and what happened instead, including relevant log output.

## Pull requests

Pull requests go against the `master` branch, the only branch in this fork.

1. Keep each pull request focused on one change, with a commit message that explains why the
   change is needed.
2. Build Dynomite and run the functional tests, as described in [README.md](README.md#tests):

       $ test/run_tests.sh

3. [CI](https://github.com/zcalusic/dynomite/actions/workflows/ci.yml) builds Dynomite and runs
   the same tests on Debian trixie for every pull request; it needs to pass.

## Documentation

The upstream [wiki](https://github.com/Netflix/dynomite/wiki) still documents Dynomite's
architecture and features. Where it and this fork's [README.md](README.md) disagree, the README
describes this fork.
