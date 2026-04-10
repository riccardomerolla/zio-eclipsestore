package io.github.riccardomerolla.zio.eclipsestore.config

import java.nio.file.Path

final case class NativeLocalEventingConfig(
  baseDir: Path,
  serde: NativeLocalSerde = NativeLocalSerde.Json,
)
