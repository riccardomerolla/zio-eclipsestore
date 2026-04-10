# SchemaSerializer

`SchemaSerializer` is the public Scala-native serialization boundary for `zio-eclipsestore`.

It keeps ZIO Schema as the only user-facing contract for:

- encoding domain values
- decoding stored payloads
- deriving stable `TypeHandler[A]` registrations
- enriching `EclipseStoreConfig` so handler wiring happens before the storage manager starts

The intended usage pattern is:

```scala
val serializer = SchemaSerializerLive()

for
  config  <- serializer.register[MyValue](EclipseStoreConfig.make(path))
  payload <- serializer.encode(myValue)
  value   <- serializer.decode[MyValue](payload)
yield (config, value)
```

Operational rules:

- userland code should depend on `Schema[A]`, not Java reflection or raw EclipseStore handlers
- `TypeHandler[A]` is the Scala wrapper for the underlying binary handler
- handler registration is automatic through `SchemaSerializer.register`
- `TypedStore.handlersFor` delegates to the same registration path so the repository has one schema-registration surface
