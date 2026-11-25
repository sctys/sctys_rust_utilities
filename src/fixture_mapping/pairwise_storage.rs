//! Shared ClickHouse DDL templates for pairwise mapping state.

use crate::click_house::{ClickHouse, ClickHouseType};
use sea_query::{Alias, Table};

use super::pairwise_lifecycle::{GENERATIONS_TABLE, MAPPING_FORMAT_ID};

pub(super) fn generations_ddl() -> clickhouse::error::Result<String> {
    const SEQUENCE: &str = "sequence";
    use ClickHouseType as Type;
    let ddl = table_ddl(
        GENERATIONS_TABLE,
        &[
            ("generation", Type::Uuid(false)),
            ("parent_generation", Type::Uuid(false)),
            ("snapshot_generation", Type::Uuid(false)),
            (SEQUENCE, Type::Int64(false)),
            ("details", Type::String(false)),
        ],
        &[SEQUENCE],
    )?;
    // SeaQuery does not render ClickHouse table comments.
    Ok(format!("{ddl} COMMENT '{MAPPING_FORMAT_ID}'"))
}

/// Render one MergeTree table using the shared ClickHouse schema rules.
pub fn table_ddl(
    table: impl std::fmt::Display,
    fields: &[(&str, ClickHouseType)],
    key: &[&str],
) -> clickhouse::error::Result<String> {
    let mut query = Table::create();
    query.table(Alias::new(table.to_string())).if_not_exists();
    for &(name, kind) in fields {
        query.col(ClickHouse::type_column(Alias::new(name), kind));
    }
    ClickHouse::render_merge_tree_ddl(
        query.take(),
        &key.iter().map(|name| Alias::new(*name)).collect::<Vec<_>>(),
    )
}

/// Pairwise-state schema shared by football and basketball.
///
/// Provider columns are supplied in public order. Only the membership table is
/// provider-wide; every other physical table uses the same schema in both sports.
pub fn table_schemas(provider_names: &[String]) -> clickhouse::error::Result<Vec<String>> {
    const GEN: &str = "generation";
    const PARENT: &str = "parent_generation";
    const ID: &str = "central_fixture_id";
    const SEQ: &str = "sequence";
    const DETAILS: &str = "details";
    const S1: &str = "source1";
    const S2: &str = "source2";
    const F1: &str = "fixture_id1_str";
    const F2: &str = "fixture_id2_str";
    const COUNT: &str = "mapping_count";
    const FLIP: &str = "reversed";
    const SIGNATURE: &str = "signature";
    const REPAIR: &str = "repair_direct";
    const REVISION: &str = "revision";

    use ClickHouseType as Type;

    let gen = (GEN, Type::Uuid(false));
    let parent = (PARENT, Type::Uuid(false));
    let id = (ID, Type::Uuid(false));
    let string = |name| (name, Type::String(false));
    let link_fields = [
        gen,
        string(S1),
        string(S2),
        string(F1),
        string(F2),
        (COUNT, Type::Int64(false)),
        (FLIP, Type::UInt8(false)),
    ];
    let mut memberships = vec![gen, id];
    memberships.extend(
        provider_names
            .iter()
            .map(|name| (name.as_str(), Type::String(true))),
    );

    Ok(vec![
        generations_ddl()?,
        table_ddl(
            "mapping_pairwise_attempts",
            &[
                parent,
                (SEQ, Type::Int64(false)),
                gen,
                string(SIGNATURE),
                (REPAIR, Type::UInt8(false)),
            ],
            &[PARENT, SEQ],
        )?,
        table_ddl(
            "mapping_pairwise_checkpoints",
            &[string(SIGNATURE), string("stage"), string(DETAILS)],
            &[SIGNATURE, "stage"],
        )?,
        table_ddl("mapping_pairwise_memberships", &memberships, &[GEN, ID])?,
        table_ddl("mapping_pairwise_touched_rows", &[gen, id], &[GEN, ID])?,
        table_ddl(
            "mapping_pairwise_lookup",
            &[gen, string("source"), string("source_fixture_id"), id],
            &[GEN, "source", "source_fixture_id"],
        )?,
        table_ddl(
            "mapping_pairwise_rectified_edges",
            &link_fields,
            &[GEN, S1, S2, F1, F2],
        )?,
        table_ddl(
            "mapping_pairwise_conflicts",
            &link_fields,
            &[GEN, S1, S2, F1, F2],
        )?,
        table_ddl(
            "mapping_pairwise_pair_revisions",
            &[gen, string(S1), string(S2), (REVISION, Type::Uuid(false))],
            &[GEN, S1, S2],
        )?,
        table_ddl(
            "mapping_pairwise_transactions",
            &[
                gen,
                parent,
                ("transaction_id", Type::Uuid(false)),
                string("before_rows"),
                string("after_rows"),
            ],
            &[GEN, "transaction_id"],
        )?,
    ])
}

/// Render the public pairwise mapping views shared by both sports.
///
/// `legacy_lookup` is an optional football-only fallback supplied by the caller.
/// `install_cross_alias` preserves football's compatibility rule for a preexisting
/// object named `mapping_cross_sources_map`.
pub fn view_statements(
    provider_names: &[String],
    legacy_lookup: Option<sea_query::SelectStatement>,
    install_cross_alias: bool,
) -> Vec<String> {
    use sea_query::{Order, SelectStatement, UnionType};

    const GEN: &str = "generation";
    const SNAPSHOT: &str = "snapshot_generation";
    const SEQ: &str = "sequence";
    const ID: &str = "central_fixture_id";
    const SOURCE: &str = "source";
    const FIXTURE: &str = "source_fixture_id";
    const S1: &str = "source1";
    const S2: &str = "source2";
    const F1: &str = "fixture_id1_str";
    const F2: &str = "fixture_id2_str";
    const COUNT: &str = "mapping_count";
    const FLIP: &str = "reversed";

    let alias = |name: &str| Alias::new(name);
    let latest = || {
        Query::select()
            .column(alias(GEN))
            .from(alias("mapping_pairwise_generations"))
            .order_by(alias(SEQ), Order::Desc)
            .limit(1)
            .to_owned()
    };
    let latest_snapshot = Query::select()
        .column(alias(SNAPSHOT))
        .from(alias("mapping_pairwise_generations"))
        .order_by(alias(SEQ), Order::Desc)
        .limit(1)
        .to_owned();
    let snapshot = |table: &str| {
        Query::select()
            .from(alias(table))
            .and_where(Expr::col(alias(GEN)).in_subquery(latest_snapshot.clone()))
            .to_owned()
    };
    let mut wide = snapshot("mapping_pairwise_memberships");
    wide.column(alias(ID))
        .columns(provider_names.iter().map(|name| alias(name)));
    let mut lookup = snapshot("mapping_pairwise_lookup");
    lookup.columns([SOURCE, FIXTURE, ID].map(alias));
    if let Some(mut fallback) = legacy_lookup {
        lookup.union(
            UnionType::All,
            fallback.and_where(Expr::exists(latest()).not()).to_owned(),
        );
    }
    let mut transactions = Query::select();
    transactions
        .expr(Expr::col(sea_query::Asterisk))
        .from(alias("mapping_pairwise_transactions"))
        .and_where(
            Expr::col(alias(GEN)).in_subquery(
                Query::select()
                    .column(alias(GEN))
                    .from(alias("mapping_pairwise_generations"))
                    .to_owned(),
            ),
        );
    let revision_set = Query::select()
        .from(alias("mapping_pairwise_pair_revisions"))
        .and_where(Expr::col(alias(GEN)).in_subquery(latest()))
        .columns(["revision", S1, S2].map(alias))
        .to_owned();
    let by_revision = |table: &str| {
        Query::select()
            .from(alias(table))
            .and_where(
                Expr::tuple([GEN, S1, S2].map(|column| Expr::col(alias(column)).into()))
                    .in_subquery(revision_set.clone()),
            )
            .to_owned()
    };
    let cross = by_revision("mapping_fixture_evidence")
        .columns(
            [
                S1,
                S2,
                F1,
                F2,
                "mid_source",
                "intermediate_fixture_id",
                "left_match_condition",
                "right_match_condition",
            ]
            .map(alias),
        )
        .and_where(Expr::col(alias("row_kind")).eq("edge"))
        .to_owned();
    let rectified = by_revision("mapping_pairwise_rectified_edges")
        .columns([S1, S2, F1, F2, COUNT, FLIP].map(alias))
        .to_owned();
    let conflicts = snapshot("mapping_pairwise_conflicts")
        .columns([S1, S2, F1, F2, COUNT, FLIP].map(alias))
        .to_owned();
    let direct = by_revision("mapping_fixture_evidence")
        .columns([S1, S2, F1, F2, "left_match_condition"].map(alias))
        .and_where(Expr::col(alias("row_kind")).eq("direct"))
        .to_owned();

    let mut views: Vec<(&str, SelectStatement)> = vec![
        ("mapping_central_fixtures", wide),
        ("mapping_fixture_central_ids", lookup),
        ("mapping_central_fixture_transactions", transactions),
        ("mapping_cross_source_links", cross.clone()),
        (
            "mapping_current_fixture_evidence",
            by_revision("mapping_fixture_evidence")
                .expr(Expr::col(sea_query::Asterisk))
                .to_owned(),
        ),
        ("mapping_rectified_fixture_links", rectified),
        ("mapping_central_fixture_conflicts", conflicts),
        ("mapping_direct_fixture_links", direct),
    ];
    if install_cross_alias {
        views.push(("mapping_cross_sources_map", cross));
    }
    views
        .into_iter()
        .map(|(view, query)| {
            format!(
                "CREATE OR REPLACE VIEW `{view}` AS {}",
                query.to_string(MysqlQueryBuilder)
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{table_schemas, view_statements};

    #[test]
    fn common_pairwise_schemas_are_identical_and_provider_order_is_preserved() {
        let make_names = |count| {
            (0..count)
                .map(|index| format!("provider_{index:02}"))
                .collect::<Vec<_>>()
        };
        let football = table_schemas(&make_names(12)).unwrap();
        let basketball = table_schemas(&make_names(14)).unwrap();

        assert_eq!(football.len(), basketball.len());
        for (index, (football_sql, basketball_sql)) in football.iter().zip(&basketball).enumerate()
        {
            if index != 3 {
                assert_eq!(football_sql, basketball_sql);
            }
        }
        let membership = &football[3];
        let previous = membership.find("provider_00").unwrap();
        for name in make_names(12).iter().skip(1) {
            let next = membership.find(name).unwrap();
            assert!(previous < next);
        }
        assert!(basketball[3].contains("provider_13"));
    }

    #[test]
    fn pairwise_views_are_shared_and_keep_football_compatibility_hooks() {
        use sea_query::{Alias, Query};

        let make_names = |count| {
            (0..count)
                .map(|index| format!("provider_{index:02}"))
                .collect::<Vec<_>>()
        };
        let football_names = make_names(12);
        let basketball_names = make_names(14);
        let football = view_statements(&football_names, None, true);
        let basketball = view_statements(&basketball_names, None, true);

        assert_eq!(football.len(), 9);
        assert_eq!(basketball.len(), 9);
        for (football_sql, basketball_sql) in football.iter().zip(&basketball).skip(1) {
            assert_eq!(football_sql, basketball_sql);
        }
        assert!(football[0].contains("provider_11"));
        assert!(basketball[0].contains("provider_13"));

        let legacy_lookup = Query::select()
            .columns([
                Alias::new("source"),
                Alias::new("source_fixture_id"),
                Alias::new("central_fixture_id"),
            ])
            .from(Alias::new("mapping_legacy_registry"))
            .to_owned();
        let legacy_views = view_statements(&football_names, Some(legacy_lookup), false);
        let lookup = legacy_views
            .iter()
            .find(|statement| statement.contains("mapping_fixture_central_ids"))
            .unwrap();
        assert!(lookup.contains("UNION ALL"));
        assert!(legacy_views
            .iter()
            .all(|statement| !statement.contains("mapping_cross_sources_map")));
    }
}

use clickhouse::{error::Result, Client};
use sea_query::{Expr, MysqlQueryBuilder, Query};
use strum_macros::{Display, IntoStaticStr, VariantArray};

use super::pairwise_lifecycle::latest;

const GEN: &str = "generation";
const SOURCE: &str = "source";
const ID: &str = "central_fixture_id";
const FIXTURE: &str = "source_fixture_id";
const PARENT: &str = "parent_generation";
const S1: &str = "source1";
const S2: &str = "source2";

fn a(name: &str) -> Alias {
    Alias::new(name)
}
#[derive(Clone, Copy, Display, IntoStaticStr, VariantArray)]
#[strum(serialize_all = "snake_case")]
pub enum EntityTable {
    MappingEntityLinks,
    MappingFixtureCountryOverrideRows,
    MappingFixtureCountryOverrides,
    MappingEntityMemberships,
    MappingEntityLinkWithdrawals,
    MappingEntityTransactions,
    MappingCountryRows,
    MappingCentralCountries,
    MappingCompetitionRows,
    MappingCentralCompetitions,
    MappingTeamRows,
    MappingCentralTeams,
    MappingCentralEntityLookup,
    MappingCurrentEntityLinks,
    MappingCentralEntityTransactions,
}

impl EntityTable {
    pub fn as_str(self) -> &'static str {
        let name: &'static str = self.into();
        name
    }
}

#[derive(Clone, Copy, Display, IntoStaticStr, VariantArray)]
#[strum(serialize_all = "snake_case")]
pub enum EntityColumn {
    Details,
    LocalId1,
    LocalId2,
    SourceFixtureId,
    CentralCountryId,
    CentralCompetitionId,
    CentralTeamId,
    LocalId,
    CentralId,
    EntityType,
    BeforeRows,
    AfterRows,
    ChangeKind,
    TransactionId,
}

impl EntityColumn {
    pub fn as_str(self) -> &'static str {
        let name: &'static str = self.into();
        name
    }
}

#[derive(Clone, Copy, Display, IntoStaticStr, VariantArray)]
#[strum(serialize_all = "snake_case")]
pub enum FixtureEntityTable {
    MappingFixtureOrientationRows,
    MappingFixtureEntityRows,
    MappingFixtureEntityTransactionRows,
    MappingFixtureEntityInputStage,
    MappingFixtureEntityVoteStage,
    MappingCentralFixtureEntities,
    MappingCentralFixtureEntityTransactions,
}

impl FixtureEntityTable {
    pub fn as_str(self) -> &'static str {
        let name: &'static str = self.into();
        name
    }
}

#[derive(Clone, Copy, Display, Eq, PartialEq, IntoStaticStr, VariantArray)]
#[strum(serialize_all = "snake_case")]
pub enum FixtureEntityColumn {
    Country,
    Competition,
    Home,
    Away,
    CentralCountryId,
    CentralCompetitionId,
    CentralHomeTeamId,
    CentralAwayTeamId,
    CountryStatus,
    CompetitionStatus,
    HomeTeamStatus,
    AwayTeamStatus,
    CountryClass,
    CountryIdConflict,
    StartTime,
    Provider,
    Known,
    Flipped,
    TransactionId,
    BeforeRows,
    AfterRows,
}

impl FixtureEntityColumn {
    pub fn as_str(self) -> &'static str {
        let name: &'static str = self.into();
        name
    }
}

pub const LOCAL_FIELDS: [FixtureEntityColumn; 4] = [
    FixtureEntityColumn::Country,
    FixtureEntityColumn::Competition,
    FixtureEntityColumn::Home,
    FixtureEntityColumn::Away,
];
const ENTITY_FIELDS: [FixtureEntityColumn; 4] = [
    FixtureEntityColumn::CentralCountryId,
    FixtureEntityColumn::CentralCompetitionId,
    FixtureEntityColumn::CentralHomeTeamId,
    FixtureEntityColumn::CentralAwayTeamId,
];
const STATUS_FIELDS: [FixtureEntityColumn; 4] = [
    FixtureEntityColumn::CountryStatus,
    FixtureEntityColumn::CompetitionStatus,
    FixtureEntityColumn::HomeTeamStatus,
    FixtureEntityColumn::AwayTeamStatus,
];

pub async fn create_entity(client: &Client, providers: &[String]) -> Result<()> {
    let string = ClickHouseType::String(false);
    let guid = ClickHouseType::Uuid(false);
    for sql in [
        table_ddl(
            EntityTable::MappingEntityLinks.as_str(),
            &[
                (GEN, guid),
                (EntityColumn::EntityType.as_str(), string),
                (S1, string),
                (S2, string),
                (EntityColumn::LocalId1.as_str(), string),
                (EntityColumn::LocalId2.as_str(), string),
                (EntityColumn::Details.as_str(), string),
            ],
            &[
                GEN,
                EntityColumn::EntityType.as_str(),
                S1,
                S2,
                EntityColumn::LocalId1.as_str(),
                EntityColumn::LocalId2.as_str(),
            ],
        )?,
        table_ddl(
            EntityTable::MappingEntityMemberships.as_str(),
            &[
                (GEN, guid),
                (EntityColumn::EntityType.as_str(), string),
                (SOURCE, string),
                (EntityColumn::LocalId.as_str(), string),
                (EntityColumn::CentralId.as_str(), guid),
            ],
            &[
                GEN,
                EntityColumn::EntityType.as_str(),
                SOURCE,
                EntityColumn::LocalId.as_str(),
            ],
        )?,
        table_ddl(
            EntityTable::MappingFixtureCountryOverrideRows.as_str(),
            &[
                (GEN, guid),
                (EntityColumn::CentralCountryId.as_str(), guid),
                (SOURCE, string),
                (EntityColumn::SourceFixtureId.as_str(), string),
            ],
            &[
                GEN,
                EntityColumn::CentralCountryId.as_str(),
                SOURCE,
                EntityColumn::SourceFixtureId.as_str(),
            ],
        )?,
        table_ddl(
            EntityTable::MappingEntityLinkWithdrawals.as_str(),
            &[(GEN, guid), (EntityColumn::Details.as_str(), string)],
            &[GEN],
        )?,
        table_ddl(
            EntityTable::MappingEntityTransactions.as_str(),
            &[
                (GEN, guid),
                (PARENT, guid),
                (EntityColumn::EntityType.as_str(), string),
                (EntityColumn::ChangeKind.as_str(), string),
                (EntityColumn::TransactionId.as_str(), guid),
                (EntityColumn::BeforeRows.as_str(), string),
                (EntityColumn::AfterRows.as_str(), string),
                (EntityColumn::Details.as_str(), string),
            ],
            &[
                GEN,
                EntityColumn::EntityType.as_str(),
                EntityColumn::TransactionId.as_str(),
            ],
        )?,
    ] {
        client.query(&sql).execute().await?;
    }
    for (table, view, id) in [
        (
            EntityTable::MappingCountryRows,
            EntityTable::MappingCentralCountries,
            EntityColumn::CentralCountryId,
        ),
        (
            EntityTable::MappingCompetitionRows,
            EntityTable::MappingCentralCompetitions,
            EntityColumn::CentralCompetitionId,
        ),
        (
            EntityTable::MappingTeamRows,
            EntityTable::MappingCentralTeams,
            EntityColumn::CentralTeamId,
        ),
    ] {
        let mut fields = vec![(GEN, guid), (ID, guid)];
        fields.extend(
            providers
                .iter()
                .map(|provider| (provider.as_str(), ClickHouseType::String(true))),
        );
        client
            .query(&table_ddl(table.as_str(), &fields, &[GEN, ID])?)
            .execute()
            .await?;
        let sql = Query::select()
            .expr_as(Expr::col(a(ID)), a(id.as_str()))
            .columns(providers.iter().map(|name| a(name)))
            .from(a(table.as_str()))
            .and_where(Expr::col(a(GEN)).in_subquery(latest()))
            .to_string(MysqlQueryBuilder);
        create_view(client, view.as_str(), &sql).await?;
    }
    let committed = Query::select()
        .column(a(GEN))
        .from(a(GENERATIONS_TABLE))
        .to_owned();
    for (view, table, generations) in [
        (
            EntityTable::MappingCentralEntityLookup.as_str(),
            EntityTable::MappingEntityMemberships.as_str(),
            latest(),
        ),
        (
            EntityTable::MappingFixtureCountryOverrides.as_str(),
            EntityTable::MappingFixtureCountryOverrideRows.as_str(),
            latest(),
        ),
        (
            EntityTable::MappingCurrentEntityLinks.as_str(),
            EntityTable::MappingEntityLinks.as_str(),
            latest(),
        ),
        (
            EntityTable::MappingCentralEntityTransactions.as_str(),
            EntityTable::MappingEntityTransactions.as_str(),
            committed,
        ),
    ] {
        let sql = Query::select()
            .expr(Expr::col(sea_query::Asterisk))
            .from(a(table))
            .and_where(Expr::col(a(GEN)).in_subquery(generations))
            .to_string(MysqlQueryBuilder);
        create_view(client, view, &sql).await?;
    }
    Ok(())
}

async fn create_view(client: &Client, view: &str, select: &str) -> Result<()> {
    // SeaQuery does not render ClickHouse CREATE VIEW.
    client
        .query(&format!("CREATE OR REPLACE VIEW `{view}` AS {select}"))
        .execute()
        .await
}

pub async fn create_fixture_entities(client: &Client) -> Result<()> {
    let guid = ClickHouseType::Uuid(false);
    let string = ClickHouseType::String(false);
    let byte = ClickHouseType::UInt8(false);
    let mask = ClickHouseType::Int32(false);
    let key = [(GEN, guid), (ID, guid)];
    let mut entities = key.to_vec();
    entities.extend(ENTITY_FIELDS.map(|field| (field.as_str(), ClickHouseType::Uuid(true))));
    let mut assignments = entities.clone();
    assignments.extend(STATUS_FIELDS.map(|field| (field.as_str(), string)));
    assignments.extend([
        (FixtureEntityColumn::Known.as_str(), mask),
        (FixtureEntityColumn::Flipped.as_str(), mask),
    ]);
    entities.extend([
        (FixtureEntityColumn::Provider.as_str(), byte),
        (FixtureEntityColumn::CountryClass.as_str(), byte),
    ]);
    let mut inputs = vec![(GEN, guid), (SOURCE, string), (FIXTURE, string)];
    inputs.extend(LOCAL_FIELDS.map(|field| (field.as_str(), string)));
    inputs.push((
        FixtureEntityColumn::StartTime.as_str(),
        ClickHouseType::Int64(false),
    ));
    inputs.push((FixtureEntityColumn::CountryClass.as_str(), byte));

    for sql in [
        table_ddl(
            FixtureEntityTable::MappingFixtureOrientationRows.as_str(),
            &[
                key[0],
                key[1],
                (FixtureEntityColumn::Known.as_str(), mask),
                (FixtureEntityColumn::Flipped.as_str(), mask),
            ],
            &[GEN, ID],
        )?,
        table_ddl(
            FixtureEntityTable::MappingFixtureEntityRows.as_str(),
            &assignments,
            &[GEN, ID],
        )?,
        table_ddl(
            FixtureEntityTable::MappingFixtureEntityTransactionRows.as_str(),
            &[
                key[0],
                (PARENT, guid),
                key[1],
                (FixtureEntityColumn::TransactionId.as_str(), guid),
                (FixtureEntityColumn::BeforeRows.as_str(), string),
                (FixtureEntityColumn::AfterRows.as_str(), string),
            ],
            &[GEN, ID],
        )?,
        format!(
            "{} PARTITION BY {GEN}",
            table_ddl(
                FixtureEntityTable::MappingFixtureEntityInputStage.as_str(),
                &inputs,
                &[GEN, SOURCE, FIXTURE]
            )?
        ),
        format!(
            "{} PARTITION BY {GEN}",
            table_ddl(
                FixtureEntityTable::MappingFixtureEntityVoteStage.as_str(),
                &entities,
                &[GEN, ID, FixtureEntityColumn::Provider.as_str()]
            )?
        ),
    ] {
        client.query(&sql).execute().await?;
    }
    let committed = Query::select()
        .column(a(GEN))
        .from(a(GENERATIONS_TABLE))
        .to_owned();
    for (view, table, generations) in [
        (
            FixtureEntityTable::MappingCentralFixtureEntities.as_str(),
            FixtureEntityTable::MappingFixtureEntityRows.as_str(),
            latest(),
        ),
        (
            FixtureEntityTable::MappingCentralFixtureEntityTransactions.as_str(),
            FixtureEntityTable::MappingFixtureEntityTransactionRows.as_str(),
            committed,
        ),
    ] {
        let sql = Query::select()
            .expr(Expr::col(sea_query::Asterisk))
            .from(a(table))
            .and_where(Expr::col(a(GEN)).in_subquery(generations))
            .to_string(MysqlQueryBuilder);
        // SeaQuery does not render ClickHouse CREATE VIEW.
        client
            .query(&format!("CREATE OR REPLACE VIEW `{view}` AS {sql}"))
            .execute()
            .await?;
    }
    Ok(())
}
