# Compatibility

[← back to the README](README.md)

## What the artifacts promise

From 1.0.0 onward:

* Binary compatibility within a major version is checked by [MiMa](https://github.com/lightbend/mima) on every
  build; `versionScheme := "semver-spec"` describes what the artifacts promise.
* New methods added to `Effect` and `RsBridge` will carry default implementations, so implementing either typeclass
  yourself keeps working across minor releases.
* Deprecations get at least one minor release before removal.

## Per-release exceptions

Every deliberate break is listed here and carries a matching filter in [`mima.sbt`](mima.sbt), so nothing is waived
silently.

### 1.1.0

`WireCodecConfig` stopped being a `case class`. It is now a `final class` with a private constructor and `withX`
builders, so `WireCodecConfig(...)`, `.copy(...)`, `unapply` and the `Product` methods are gone — recompile against
`1.1.0` and use `WireCodecConfig.Default.withFieldNaming(...)`.

That break was taken deliberately and once: every derivation option added after this one is a new `withX` method,
which is binary compatible, where a new field on a `case class` would have broken `apply`/`copy` again in every
release. Everything else in `1.1.0` is binary compatible with `1.0.0`, and MiMa now runs in CI to keep it that way.

## Scala versions

`Scala 3 TASTy` is backward but not forward compatible, so `mongo4s-bson-calypso`, `mongo4s-kyo` and `mongo4s-rapid`
— the three modules built on `3.8` — **cannot be consumed from a Scala `3.3 LTS` project**, even though everything
else can. They are pinned there because their upstream dependencies require it.

`mongo4s-kyo` depends on a kyo release candidate. Until kyo reaches 1.0.0 final, that module sits outside the binary
compatibility promise the other artifacts make.

Compiling any module that touches kyo requires `JDK 25` — the `kyo.Frame` macro runs inside the compiler and its
class files target `Java 25`, so this is a compile-time requirement, not just a runtime one.
