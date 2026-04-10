package io.github.riccardomerolla.zio.eclipsestore.schema

import zio.*
import zio.json.ast.Json
import zio.schema.{ DeriveSchema, Schema }
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError

object MigrationSpec extends ZIOSpecDefault:
  final case class CustomerV1(id: String, name: String)
  object CustomerV1:
    given Schema[CustomerV1] = DeriveSchema.gen[CustomerV1]

  final case class CustomerV2(id: String, name: String, active: Boolean)
  object CustomerV2:
    given Schema[CustomerV2] = DeriveSchema.gen[CustomerV2]

  final case class ProfileV1(userId: String, fullName: String)
  object ProfileV1:
    given Schema[ProfileV1] = DeriveSchema.gen[ProfileV1]

  final case class ProfileV2(userId: String, displayName: String)
  object ProfileV2:
    given Schema[ProfileV2] = DeriveSchema.gen[ProfileV2]

  final case class AgeV1(age: String)
  object AgeV1:
    given Schema[AgeV1] = DeriveSchema.gen[AgeV1]

  final case class AgeV2(age: Int)
  object AgeV2:
    given Schema[AgeV2] = DeriveSchema.gen[AgeV2]

  final case class AccountV1(id: String, fullName: String)
  object AccountV1:
    given Schema[AccountV1] = DeriveSchema.gen[AccountV1]

  final case class AccountV2(id: String, displayName: String)
  object AccountV2:
    given Schema[AccountV2] = DeriveSchema.gen[AccountV2]

  final case class AccountV3(id: String, displayName: String, active: Boolean)
  object AccountV3:
    given Schema[AccountV3] = DeriveSchema.gen[AccountV3]

  private val addDefaultPlan =
    Migration
      .define[CustomerV1, CustomerV2](
        Migration.addField("active", Json.Bool(true))
      )
      .plan

  private val renamePlan =
    Migration
      .define[ProfileV1, ProfileV2](
        Migration.renameField("fullName", "displayName")
      )
      .plan

  private val chainedPlan =
    Migration
      .define[AccountV1, AccountV2](
        Migration.renameField("fullName", "displayName")
      )
      .andThen(
        Migration.define[AccountV2, AccountV3](
          Migration.addField("active", Json.Bool(true))
        )
      )
      .plan

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("Migration")(
      test("adds a default value when a field is introduced") {
        val input = CustomerV1("cust-1", "Ada")

        for
          _      <- addDefaultPlan.validate
          output <- addDefaultPlan.migrate(input)
        yield assertTrue(
          output == CustomerV2("cust-1", "Ada", active = true)
        )
      },
      test("renames fields without losing values") {
        val input = ProfileV1("user-1", "Ada Lovelace")

        for
          _      <- renamePlan.validate
          output <- renamePlan.migrate(input)
        yield assertTrue(
          output == ProfileV2("user-1", "Ada Lovelace")
        )
      },
      test("fails validation when a field type changes without a converter") {
        val plan = Migration.define[AgeV1, AgeV2]().plan

        for
          failure <- plan.validate.flip
        yield assertTrue(
          failure match
            case EclipseStoreError.IncompatibleSchemaError(message, _) =>
              message.contains("changed type without a converter")
            case _                                                     => false
        )
      },
      test("composes chained migrations across multiple schema versions") {
        val input = AccountV1("acct-1", "Grace Hopper")

        for
          _      <- chainedPlan.validate
          output <- chainedPlan.migrate(input)
        yield assertTrue(
          output == AccountV3("acct-1", "Grace Hopper", active = true)
        )
      },
      test("compatible upgrades load existing payloads without explicit reserialization") {
        val payloads =
          Chunk.fromIterable((1 to 256).map(index => CustomerV1(s"cust-$index", s"Customer $index")))

        for
          _        <- addDefaultPlan.validate
          upgraded <- ZIO.foreach(payloads)(addDefaultPlan.migrate)
        yield assertTrue(
          upgraded.size == payloads.size,
          upgraded.forall(_.active),
          upgraded.headOption.contains(CustomerV2("cust-1", "Customer 1", active = true)),
        )
      },
      test("validated converters preserve semantics across generated values") {
        val plan =
          Migration
            .define[AgeV1, AgeV2](
              Migration.changeField("age")(jsonValue =>
                jsonValue match
                  case Json.Str(value) =>
                    ZIO
                      .attempt(value.toInt)
                      .mapBoth(
                        cause => EclipseStoreError.MigrationError("Age converter must parse integers", Some(cause)),
                        number => Json.Num(number),
                      )
                  case other           =>
                    ZIO.fail(EclipseStoreError.MigrationError(s"Expected string age payload, found $other", None))
              )
            )
            .plan

        check(Gen.int(0, 120)) { age =>
          plan.validate *> plan.migrate(AgeV1(age.toString)).map(output => assertTrue(output.age == age))
        }
      },
    )
