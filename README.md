# octoii-siphon-lib

Public Rust interface and binary distribution for Siphon.

## Status

Under development. No usable library or binary release has been published from this repository yet.

The intended Cargo package is `octoii-siphon-lib`, imported as `octoii_siphon_lib`.
It will provide safe Rust interfaces over a versioned C ABI and precompiled platform libraries.
Consumers will not require access to the private implementation repository or compile its sources.

Required release platforms are macOS, Windows and Linux. Specific supported targets and operating-system
minimums will be listed with each verified release. Repository creation does not establish platform support.

The implementation remains in the private `devxiyang/octoii-siphon` repository. This repository contains
only the public interface, consumer documentation, integration tests and distribution metadata/artifacts.
The public wrapper must preserve Siphon's durable execution, cancellation and recovery semantics.
