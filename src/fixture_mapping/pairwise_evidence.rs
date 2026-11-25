//! Active pairwise evidence schema, publication and integrity checks.
use crate::click_house::{ClickHouse, ClickHouseType};
use clickhouse::Row;
use fxhash::FxHashSet;
use sea_query::{
    Alias, Expr, Func, IdenStatic, JoinType, MysqlQueryBuilder, OverStatement, Query,
    SelectStatement, Table, TableCreateStatement, UnionType,
};
use serde::{de::Error as _, Deserialize};
use std::time::Instant;
use strum_macros::{Display, IntoStaticStr, VariantArray};
use uuid::Uuid;

pub use super::pairwise_lifecycle::EvidenceRevision;
use super::pairwise_lifecycle::{multiset_digest, revisions_are_complete};

const GEN: &str = "generation";
const S1: &str = "source1";
const S2: &str = "source2";
const F1: &str = "fixture_id1_str";
const F2: &str = "fixture_id2_str";
const FLIP: &str = "reversed";
const COUNT: &str = "mapping_count";
const RECTIFIED_EDGES: &str = "mapping_pairwise_rectified_edges";

fn a(name: &str) -> Alias {
    Alias::new(name)
}
fn error(cause: impl std::fmt::Display) -> clickhouse::error::Error {
    clickhouse::error::Error::custom(cause.to_string())
}

pub const BRIDGE_ROW_KIND: &str = "edge";
pub const DIRECT_ROW_KIND: &str = "direct";
#[allow(dead_code)]
pub const CANDIDATE_ROW_KIND: &str = "candidate";
const EVIDENCE_INSERT_ERROR: &str = "unable to insert fixture evidence";
const EVIDENCE_COUNT_ERROR: &str = "unable to count fixture evidence rows";
const EVIDENCE_MISSING: &str = "fixture evidence is missing or incomplete";
const BUILD_PAIR_EVIDENCE_PROGRESS: &str = "Fixture evidence: building raw direct and bridge rows";
#[derive(Clone, Copy, IdenStatic)]
enum EvidenceColumn {
    Generation,
    RowKind,
    Source1,
    Source2,
    FixtureId1Str,
    FixtureId2Str,
    CountryId1Str,
    CountryId2Str,
    CompetitionId1Str,
    CompetitionId2Str,
    HomeTeamId1Str,
    HomeTeamId2Str,
    AwayTeamId1Str,
    AwayTeamId2Str,
    MidSource,
    IntermediateFixtureId,
    LeftCandidateCount,
    RightCandidateCount,
    LeftSource1,
    LeftSource2,
    LeftFixtureId1Str,
    LeftFixtureId2Str,
    LeftStartTime1,
    LeftStartTime2,
    LeftHomeScore1,
    LeftHomeScore2,
    LeftAwayScore1,
    LeftAwayScore2,
    LeftPeriod1,
    LeftPeriod2,
    LeftMatchLevel,
    LeftMatchCondition,
    LeftInputFingerprint1,
    LeftInputFingerprint2,
    RightSource1,
    RightSource2,
    RightFixtureId1Str,
    RightFixtureId2Str,
    RightStartTime1,
    RightStartTime2,
    RightHomeScore1,
    RightHomeScore2,
    RightAwayScore1,
    RightAwayScore2,
    RightPeriod1,
    RightPeriod2,
    RightMatchLevel,
    RightMatchCondition,
    RightInputFingerprint1,
    RightInputFingerprint2,
}

const LEG_EVIDENCE_COLUMNS: [EvidenceColumn; 32] = [
    EvidenceColumn::LeftSource1,
    EvidenceColumn::LeftSource2,
    EvidenceColumn::LeftFixtureId1Str,
    EvidenceColumn::LeftFixtureId2Str,
    EvidenceColumn::LeftStartTime1,
    EvidenceColumn::LeftStartTime2,
    EvidenceColumn::LeftHomeScore1,
    EvidenceColumn::LeftHomeScore2,
    EvidenceColumn::LeftAwayScore1,
    EvidenceColumn::LeftAwayScore2,
    EvidenceColumn::LeftPeriod1,
    EvidenceColumn::LeftPeriod2,
    EvidenceColumn::LeftMatchLevel,
    EvidenceColumn::LeftMatchCondition,
    EvidenceColumn::LeftInputFingerprint1,
    EvidenceColumn::LeftInputFingerprint2,
    EvidenceColumn::RightSource1,
    EvidenceColumn::RightSource2,
    EvidenceColumn::RightFixtureId1Str,
    EvidenceColumn::RightFixtureId2Str,
    EvidenceColumn::RightStartTime1,
    EvidenceColumn::RightStartTime2,
    EvidenceColumn::RightHomeScore1,
    EvidenceColumn::RightHomeScore2,
    EvidenceColumn::RightAwayScore1,
    EvidenceColumn::RightAwayScore2,
    EvidenceColumn::RightPeriod1,
    EvidenceColumn::RightPeriod2,
    EvidenceColumn::RightMatchLevel,
    EvidenceColumn::RightMatchCondition,
    EvidenceColumn::RightInputFingerprint1,
    EvidenceColumn::RightInputFingerprint2,
];

const INSERT_COLUMNS: [EvidenceColumn; 50] = [
    EvidenceColumn::Generation,
    EvidenceColumn::RowKind,
    EvidenceColumn::Source1,
    EvidenceColumn::Source2,
    EvidenceColumn::FixtureId1Str,
    EvidenceColumn::FixtureId2Str,
    EvidenceColumn::CountryId1Str,
    EvidenceColumn::CountryId2Str,
    EvidenceColumn::CompetitionId1Str,
    EvidenceColumn::CompetitionId2Str,
    EvidenceColumn::HomeTeamId1Str,
    EvidenceColumn::HomeTeamId2Str,
    EvidenceColumn::AwayTeamId1Str,
    EvidenceColumn::AwayTeamId2Str,
    EvidenceColumn::MidSource,
    EvidenceColumn::IntermediateFixtureId,
    EvidenceColumn::LeftCandidateCount,
    EvidenceColumn::RightCandidateCount,
    EvidenceColumn::LeftSource1,
    EvidenceColumn::LeftSource2,
    EvidenceColumn::LeftFixtureId1Str,
    EvidenceColumn::LeftFixtureId2Str,
    EvidenceColumn::LeftStartTime1,
    EvidenceColumn::LeftStartTime2,
    EvidenceColumn::LeftHomeScore1,
    EvidenceColumn::LeftHomeScore2,
    EvidenceColumn::LeftAwayScore1,
    EvidenceColumn::LeftAwayScore2,
    EvidenceColumn::LeftPeriod1,
    EvidenceColumn::LeftPeriod2,
    EvidenceColumn::LeftMatchLevel,
    EvidenceColumn::LeftMatchCondition,
    EvidenceColumn::LeftInputFingerprint1,
    EvidenceColumn::LeftInputFingerprint2,
    EvidenceColumn::RightSource1,
    EvidenceColumn::RightSource2,
    EvidenceColumn::RightFixtureId1Str,
    EvidenceColumn::RightFixtureId2Str,
    EvidenceColumn::RightStartTime1,
    EvidenceColumn::RightStartTime2,
    EvidenceColumn::RightHomeScore1,
    EvidenceColumn::RightHomeScore2,
    EvidenceColumn::RightAwayScore1,
    EvidenceColumn::RightAwayScore2,
    EvidenceColumn::RightPeriod1,
    EvidenceColumn::RightPeriod2,
    EvidenceColumn::RightMatchLevel,
    EvidenceColumn::RightMatchCondition,
    EvidenceColumn::RightInputFingerprint1,
    EvidenceColumn::RightInputFingerprint2,
];

const ORDER_BY: [EvidenceColumn; 8] = [
    EvidenceColumn::Generation,
    EvidenceColumn::Source1,
    EvidenceColumn::Source2,
    EvidenceColumn::FixtureId1Str,
    EvidenceColumn::FixtureId2Str,
    EvidenceColumn::RowKind,
    EvidenceColumn::MidSource,
    EvidenceColumn::IntermediateFixtureId,
];

#[derive(Deserialize, Row)]
struct EvidenceCount {
    count: u64,
}

#[derive(Deserialize, Row)]
struct EvidenceIntegrity {
    rows: u64,
    hash_sum: u64,
    hash_xor: u64,
}

#[derive(Clone, Copy, Display, IntoStaticStr, VariantArray)]
#[strum(serialize_all = "snake_case")]
pub enum EvidenceTable {
    MappingFixtureEvidence,
    MappingFixtureEvidencePartitionMigration,
}

impl EvidenceTable {
    pub fn as_str(self) -> &'static str {
        let name: &'static str = self.into();
        name
    }
}

#[derive(Clone, Copy, Display, IntoStaticStr, VariantArray)]
#[strum(serialize_all = "snake_case")]
enum EvidenceAlias {
    CandidatePairs,
    CountedCandidates,
    CrossSourceEvidence,
    LeftCount,
    RightCount,
    MappingCount,
}

impl EvidenceAlias {
    fn as_str(self) -> &'static str {
        let name: &'static str = self.into();
        name
    }
}

const PARTITION_EXPRESSION: &str = "toString(generation)";

pub async fn create_table(
    click_house: &ClickHouse<'_>,
    client: &clickhouse::Client,
) -> clickhouse::error::Result<()> {
    create_partitioned(
        click_house,
        client,
        EvidenceTable::MappingFixtureEvidence.as_str(),
    )
    .await
}

async fn create_partitioned(
    click_house: &ClickHouse<'_>,
    client: &clickhouse::Client,
    table: &str,
) -> clickhouse::error::Result<()> {
    click_house
        .sql_execution(client, &partitioned_ddl(table)?)
        .await
}

pub fn partitioned_ddl(table: &str) -> clickhouse::error::Result<String> {
    let mut schema = evidence_schema();
    schema.table(Alias::new(table));
    let statement = ClickHouse::render_merge_tree_ddl(schema, &ORDER_BY).map_err(error)?;
    // SeaQuery does not model ClickHouse partition keys.
    Ok(format!("{statement} PARTITION BY {PARTITION_EXPRESSION}"))
}

pub async fn prepare_retention(
    click_house: &ClickHouse<'_>,
    client: &clickhouse::Client,
    revisions: &[EvidenceRevision],
) -> clickhouse::error::Result<()> {
    create_table(click_house, client).await?;
    let sql = Query::select()
        .column(a("partition_key"))
        .from((a("system"), a("tables")))
        .and_where(Expr::col(a("database")).eq(Func::cust(a("currentDatabase")).args([])))
        .and_where(Expr::col(a("name")).eq(EvidenceTable::MappingFixtureEvidence.as_str()))
        .to_string(MysqlQueryBuilder);
    let partition_key = client.query(&sql).fetch_one::<String>().await?;
    let drop_migration = Table::drop()
        .table(a(
            EvidenceTable::MappingFixtureEvidencePartitionMigration.as_str()
        ))
        .if_exists()
        .to_string(MysqlQueryBuilder);
    if partition_key == PARTITION_EXPRESSION {
        return Ok(());
    }
    if !partition_key.is_empty() {
        return Err(error("fixture evidence has an unexpected partition key"));
    }
    client.query(&drop_migration).execute().await?;
    create_partitioned(
        click_house,
        client,
        EvidenceTable::MappingFixtureEvidencePartitionMigration.as_str(),
    )
    .await?;
    for revision in revisions {
        let filter = |query: &mut SelectStatement| {
            query
                .and_where(Expr::col(EvidenceColumn::Generation).eq(uuid_expr(revision.generation)))
                .and_where(Expr::col(EvidenceColumn::Source1).eq(&revision.source1))
                .and_where(Expr::col(EvidenceColumn::Source2).eq(&revision.source2));
        };
        let mut select = Query::select();
        select
            .columns(INSERT_COLUMNS)
            .from(a(EvidenceTable::MappingFixtureEvidence.as_str()));
        filter(&mut select);
        let mut insert = Query::insert();
        insert
            .into_table(a(
                EvidenceTable::MappingFixtureEvidencePartitionMigration.as_str()
            ))
            .columns(INSERT_COLUMNS);
        insert.select_from(select).map_err(error)?;
        client
            .query(&insert.to_string(MysqlQueryBuilder))
            .execute()
            .await?;
        let mut count = Query::select();
        count
            .expr(Func::count(Expr::col(sea_query::Asterisk)))
            .from(a(
                EvidenceTable::MappingFixtureEvidencePartitionMigration.as_str()
            ));
        filter(&mut count);
        if client
            .query(&count.to_string(MysqlQueryBuilder))
            .fetch_one::<u64>()
            .await?
            != revision.rows
        {
            return Err(error(
                "migrated fixture evidence does not match the active revision",
            ));
        }
    }
    // EXCHANGE TABLES atomically swaps the rebuilt table into the existing interface.
    client
        .query(&format!(
            "EXCHANGE TABLES `{evidence_table}` AND `{migration_table}`",
            evidence_table = EvidenceTable::MappingFixtureEvidence.as_str(),
            migration_table = EvidenceTable::MappingFixtureEvidencePartitionMigration.as_str(),
        ))
        .execute()
        .await?;
    Ok(())
}

pub async fn finish_migration(client: &clickhouse::Client) -> clickhouse::error::Result<()> {
    let drop_migration = Table::drop()
        .table(Alias::new(
            EvidenceTable::MappingFixtureEvidencePartitionMigration.as_str(),
        ))
        .if_exists()
        .to_string(MysqlQueryBuilder);
    client.query(&drop_migration).execute().await
}

pub async fn prune(
    client: &clickhouse::Client,
    revisions: &[EvidenceRevision],
) -> clickhouse::error::Result<()> {
    let protected: FxHashSet<_> = revisions
        .iter()
        .map(|revision| revision.generation)
        .collect();
    prune_partitions(client, |id| !protected.contains(&id)).await?;
    Ok(())
}

pub async fn prune_partitions(
    client: &clickhouse::Client,
    remove: impl Fn(Uuid) -> bool,
) -> clickhouse::error::Result<usize> {
    if !table_exists(client).await? {
        return Ok(0);
    }
    #[derive(Row, Deserialize)]
    struct Partition {
        partition: String,
        partition_id: String,
    }
    let sql = Query::select()
        .distinct()
        .columns([a("partition"), a("partition_id")])
        .from((a("system"), a("parts")))
        .and_where(Expr::col(a("database")).eq(Func::cust(a("currentDatabase")).args([])))
        .and_where(Expr::col(a("table")).eq(EvidenceTable::MappingFixtureEvidence.as_str()))
        .and_where(Expr::col(a("active")).eq(true))
        .to_string(MysqlQueryBuilder);
    let partitions = client.query(&sql).fetch_all::<Partition>().await?;
    let mut dropped = 0;
    for Partition {
        partition,
        partition_id: id,
    } in partitions
    {
        if id == "all" {
            // Old installations can restore before their first retention migration.
            continue;
        }
        let generation = Uuid::parse_str(&partition).map_err(error)?;
        if !remove(generation) {
            continue;
        }
        if id.is_empty()
            || !id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(error("unexpected fixture evidence partition ID"));
        }
        // SeaQuery does not model ALTER TABLE DROP PARTITION.
        client
            .query(&format!(
                "ALTER TABLE `{evidence_table}` DROP PARTITION ID '{id}'",
                evidence_table = EvidenceTable::MappingFixtureEvidence.as_str(),
            ))
            .execute()
            .await?;
        dropped += 1;
    }
    Ok(dropped)
}

fn evidence_schema() -> TableCreateStatement {
    let mut create = Table::create();
    create
        .table(Alias::new(EvidenceTable::MappingFixtureEvidence.as_str()))
        .if_not_exists()
        .col(ClickHouse::type_column(
            EvidenceColumn::Generation,
            ClickHouseType::Uuid(false),
        ))
        .col(ClickHouse::type_column(
            EvidenceColumn::RowKind,
            ClickHouseType::String(false),
        ))
        .col(ClickHouse::type_column(
            EvidenceColumn::Source1,
            ClickHouseType::String(false),
        ))
        .col(ClickHouse::type_column(
            EvidenceColumn::Source2,
            ClickHouseType::String(false),
        ));
    for column in [
        EvidenceColumn::FixtureId1Str,
        EvidenceColumn::FixtureId2Str,
        EvidenceColumn::CountryId1Str,
        EvidenceColumn::CountryId2Str,
        EvidenceColumn::CompetitionId1Str,
        EvidenceColumn::CompetitionId2Str,
        EvidenceColumn::HomeTeamId1Str,
        EvidenceColumn::HomeTeamId2Str,
        EvidenceColumn::AwayTeamId1Str,
        EvidenceColumn::AwayTeamId2Str,
        EvidenceColumn::MidSource,
        EvidenceColumn::IntermediateFixtureId,
    ] {
        create.col(ClickHouse::type_column(
            column,
            ClickHouseType::String(false),
        ));
    }
    create.col(ClickHouse::type_column(
        EvidenceColumn::LeftCandidateCount,
        ClickHouseType::Int64(false),
    ));
    create.col(ClickHouse::type_column(
        EvidenceColumn::RightCandidateCount,
        ClickHouseType::Int64(false),
    ));
    for column in LEG_EVIDENCE_COLUMNS {
        let column_type = match column {
            EvidenceColumn::LeftStartTime1
            | EvidenceColumn::LeftStartTime2
            | EvidenceColumn::RightStartTime1
            | EvidenceColumn::RightStartTime2 => ClickHouseType::Int64(false),
            EvidenceColumn::LeftHomeScore1
            | EvidenceColumn::LeftHomeScore2
            | EvidenceColumn::LeftAwayScore1
            | EvidenceColumn::LeftAwayScore2
            | EvidenceColumn::RightHomeScore1
            | EvidenceColumn::RightHomeScore2
            | EvidenceColumn::RightAwayScore1
            | EvidenceColumn::RightAwayScore2 => ClickHouseType::Int32(false),
            _ => ClickHouseType::String(false),
        };
        create.col(ClickHouse::type_column(column, column_type));
    }
    create
}

// Canonical fixture IDs determine the vote, even when metadata varies between witnesses.
pub fn rectified_links(
    generation: Uuid,
    source1: &str,
    source2: &str,
    reversed: &str,
) -> SelectStatement {
    let left = Expr::col(EvidenceColumn::LeftMatchCondition).eq(reversed);
    let right = Expr::col(EvidenceColumn::RightMatchCondition).eq(reversed);
    let orientation = Func::cust(Alias::new("toUInt8"))
        .arg(Func::cust(Alias::new("notEquals")).args([left, right]));
    let candidates = Query::select()
        .columns([EvidenceColumn::FixtureId1Str, EvidenceColumn::FixtureId2Str])
        .expr_as(
            Func::cust(Alias::new("uniqExact")).arg(Expr::col(EvidenceColumn::MidSource)),
            Alias::new(EvidenceAlias::MappingCount.as_str()),
        )
        .expr_as(Func::min(orientation.clone()), Alias::new("reversed"))
        .expr_as(Func::max(orientation), Alias::new("max_reversed"))
        .from(Alias::new(EvidenceTable::MappingFixtureEvidence.as_str()))
        .and_where(Expr::col(EvidenceColumn::Generation).eq(uuid_expr(generation)))
        .and_where(Expr::col(EvidenceColumn::Source1).eq(source1))
        .and_where(Expr::col(EvidenceColumn::Source2).eq(source2))
        .and_where(Expr::col(EvidenceColumn::RowKind).is_in([DIRECT_ROW_KIND, BRIDGE_ROW_KIND]))
        .group_by_columns([EvidenceColumn::FixtureId1Str, EvidenceColumn::FixtureId2Str])
        .to_owned();

    let rank = |column| {
        sea_query::WindowStatement::new()
            .partition_by(column)
            .order_by(
                Alias::new(EvidenceAlias::MappingCount.as_str()),
                sea_query::Order::Desc,
            )
            .take()
    };
    let maxima = Query::select()
        .expr(Expr::col(sea_query::Asterisk))
        .expr_window_as(
            Expr::cust("rank()"),
            rank(EvidenceColumn::FixtureId1Str),
            Alias::new("left_rank"),
        )
        .expr_window_as(
            Expr::cust("rank()"),
            rank(EvidenceColumn::FixtureId2Str),
            Alias::new("right_rank"),
        )
        .from_subquery(
            candidates,
            Alias::new(EvidenceAlias::CandidatePairs.as_str()),
        )
        .to_owned();
    let count = |column| {
        sea_query::WindowStatement::new()
            .partition_by(column)
            .take()
    };
    let strongest = Query::select()
        .expr(Expr::col(sea_query::Asterisk))
        .expr_window_as(
            Func::count(Expr::col(sea_query::Asterisk)),
            count(EvidenceColumn::FixtureId1Str),
            Alias::new(EvidenceAlias::LeftCount.as_str()),
        )
        .expr_window_as(
            Func::count(Expr::col(sea_query::Asterisk)),
            count(EvidenceColumn::FixtureId2Str),
            Alias::new(EvidenceAlias::RightCount.as_str()),
        )
        .from_subquery(maxima, Alias::new("ranked_candidates"))
        .and_where(Expr::col(Alias::new("left_rank")).eq(1))
        .and_where(Expr::col(Alias::new("right_rank")).eq(1))
        .to_owned();
    Query::select()
        .columns([EvidenceColumn::FixtureId1Str, EvidenceColumn::FixtureId2Str])
        .columns([
            Alias::new(EvidenceAlias::MappingCount.as_str()),
            Alias::new("reversed"),
        ])
        .from_subquery(
            strongest,
            Alias::new(EvidenceAlias::CountedCandidates.as_str()),
        )
        .and_where(Expr::col(Alias::new(EvidenceAlias::LeftCount.as_str())).eq(1))
        .and_where(Expr::col(Alias::new(EvidenceAlias::RightCount.as_str())).eq(1))
        .and_where(Expr::col(Alias::new("reversed")).equals(Alias::new("max_reversed")))
        .to_owned()
}

pub async fn publish_rendered_pair(
    generation: Uuid,
    source1: &str,
    source2: &str,
    statement: &str,
    click_house: &ClickHouse<'_>,
    client: &clickhouse::Client,
) -> clickhouse::error::Result<EvidenceRevision> {
    let started = Instant::now();
    click_house.get_logger().log_info(&format!(
        "{BUILD_PAIR_EVIDENCE_PROGRESS} for {source1}/{source2}"
    ));
    click_house
        .sql_execution(client, statement)
        .await
        .map_err(|error| {
            clickhouse::error::Error::custom(format!("{EVIDENCE_INSERT_ERROR}: {error}"))
        })?;

    let integrity = evidence_integrity(generation, source1, source2, client).await?;
    let rows = integrity.rows;
    click_house.get_logger().log_info(&format!(
        "{BUILD_PAIR_EVIDENCE_PROGRESS} for {source1}/{source2}: {rows} rows in {:.2}s",
        started.elapsed().as_secs_f64()
    ));
    Ok(EvidenceRevision {
        source1: source1.to_owned(),
        source2: source2.to_owned(),
        generation,
        rows,
        raw_digest: multiset_digest(integrity.rows, integrity.hash_sum, integrity.hash_xor),
        rectified_rows: 0,
        rectified_digest: String::new(),
    })
}

/// Sport adapter for the direct and bridge input relations.
pub struct PairEvidenceInputs<'a> {
    pub direct_table: &'a str,
    pub providers: &'a [&'a str],
    pub direct: &'a dyn Fn() -> SelectStatement,
    pub bridge: &'a dyn Fn(&str) -> SelectStatement,
}

const FIXTURE_ID_BASE_COLUMNS: [&str; 5] = [
    "fixture_id",
    "country_id",
    "competition_id",
    "home_team_id",
    "away_team_id",
];
const MAPPING_SOURCE1_COLUMN: &str = "source1";
const MAPPING_SOURCE2_COLUMN: &str = "source2";
const MAPPING_ACTIVE_COLUMN: &str = "is_active";
const MAPPING_ACTIVE_VALUE: i32 = 1;
const LEFT_MAPPING_ALIAS: &str = "left_mapping";
const RIGHT_MAPPING_ALIAS: &str = "right_mapping";
const INTERMEDIATE_FIXTURE_ID_ALIAS: &str = "intermediate_fixture_id";

/// Build the cross-source rows from direct fixture mappings.
///
/// Each result represents two source fixtures that map to the same middle
/// fixture, with matching country, competition and team identities on that
/// middle fixture. `bridge_detail_columns` names direct mapping columns to copy
/// into the prefixed `left_*` and `right_*` evidence fields.
pub fn cross_source_mapping_query(
    fixture_table: &str,
    source1: &str,
    source2: &str,
    mid_source: &str,
    bridge_detail_columns: &[&str],
) -> SelectStatement {
    let (left_source1, left_source2) = ordered_sources(source1, mid_source);
    let (right_source1, right_source2) = ordered_sources(source2, mid_source);
    let left_query = direct_mapping_query(
        fixture_table,
        left_source1,
        left_source2,
        bridge_detail_columns,
    );
    let right_query = direct_mapping_query(
        fixture_table,
        right_source1,
        right_source2,
        bridge_detail_columns,
    );
    let left_target_side = if source1 < mid_source { 1 } else { 2 };
    let left_mid_side = if left_target_side == 1 { 2 } else { 1 };
    let right_target_side = if source2 < mid_source { 1 } else { 2 };
    let right_mid_side = if right_target_side == 1 { 2 } else { 1 };

    let mut query = Query::select();
    for (output_side, mapping_alias, input_side) in [
        (1, LEFT_MAPPING_ALIAS, left_target_side),
        (2, RIGHT_MAPPING_ALIAS, right_target_side),
    ] {
        for base in FIXTURE_ID_BASE_COLUMNS {
            query.expr_as(
                Expr::col((
                    Alias::new(mapping_alias),
                    Alias::new(id_column(base, input_side)),
                )),
                Alias::new(id_column(base, output_side)),
            );
        }
    }
    query.expr_as(
        Expr::col((
            Alias::new(LEFT_MAPPING_ALIAS),
            Alias::new(id_column("fixture_id", left_mid_side)),
        )),
        Alias::new(INTERMEDIATE_FIXTURE_ID_ALIAS),
    );
    for column in bridge_detail_columns {
        query.expr_as(
            Expr::col((Alias::new(LEFT_MAPPING_ALIAS), Alias::new(*column))),
            Alias::new(format!("left_{column}")),
        );
        query.expr_as(
            Expr::col((Alias::new(RIGHT_MAPPING_ALIAS), Alias::new(*column))),
            Alias::new(format!("right_{column}")),
        );
    }

    let mut on = Expr::col((
        Alias::new(LEFT_MAPPING_ALIAS),
        Alias::new(id_column("fixture_id", left_mid_side)),
    ))
    .equals((
        Alias::new(RIGHT_MAPPING_ALIAS),
        Alias::new(id_column("fixture_id", right_mid_side)),
    ));
    for base in FIXTURE_ID_BASE_COLUMNS.iter().skip(1) {
        on = on.and(
            Expr::col((
                Alias::new(LEFT_MAPPING_ALIAS),
                Alias::new(id_column(base, left_mid_side)),
            ))
            .equals((
                Alias::new(RIGHT_MAPPING_ALIAS),
                Alias::new(id_column(base, right_mid_side)),
            )),
        );
    }
    query
        .from_subquery(left_query, Alias::new(LEFT_MAPPING_ALIAS))
        .join_subquery(
            JoinType::InnerJoin,
            right_query,
            Alias::new(RIGHT_MAPPING_ALIAS),
            on,
        )
        .distinct()
        .to_owned()
}

fn direct_mapping_query(
    fixture_table: &str,
    source1: &str,
    source2: &str,
    bridge_detail_columns: &[&str],
) -> SelectStatement {
    let mut query = Query::select();
    for base in FIXTURE_ID_BASE_COLUMNS {
        for side in [1, 2] {
            query.column(Alias::new(id_column(base, side)));
        }
    }
    for column in bridge_detail_columns {
        query.column(Alias::new(*column));
    }
    query
        .from(Alias::new(fixture_table))
        .and_where(Expr::col(Alias::new(MAPPING_SOURCE1_COLUMN)).eq(source1))
        .and_where(Expr::col(Alias::new(MAPPING_SOURCE2_COLUMN)).eq(source2))
        .and_where(Expr::col(Alias::new(MAPPING_ACTIVE_COLUMN)).eq(MAPPING_ACTIVE_VALUE))
        .to_owned()
}

fn id_column(base: &str, side: usize) -> String {
    format!("{base}{side}_str")
}

fn ordered_sources<'a>(source1: &'a str, source2: &'a str) -> (&'a str, &'a str) {
    if source1 < source2 {
        (source1, source2)
    } else {
        (source2, source1)
    }
}

pub fn pair_insert_statement(
    generation: Uuid,
    source1: &str,
    source2: &str,
    previous: Option<Uuid>,
    changed_witnesses: &[Option<&str>],
    inputs: PairEvidenceInputs<'_>,
) -> clickhouse::error::Result<String> {
    let direct_changed = previous.is_none() || changed_witnesses.contains(&None);
    let mut parts = Vec::new();
    if let Some(previous) = previous {
        let mut reused = Query::select();
        reused
            .expr(uuid_expr(generation))
            .columns(INSERT_COLUMNS[1..].iter().copied())
            .from(Alias::new(EvidenceTable::MappingFixtureEvidence.as_str()))
            .and_where(Expr::col(EvidenceColumn::Generation).eq(uuid_expr(previous)))
            .and_where(Expr::col(EvidenceColumn::Source1).eq(source1))
            .and_where(Expr::col(EvidenceColumn::Source2).eq(source2))
            .and_where(Expr::col(EvidenceColumn::RowKind).is_in([DIRECT_ROW_KIND, BRIDGE_ROW_KIND]))
            .and_where(
                Expr::col(EvidenceColumn::MidSource).is_not_in(
                    changed_witnesses
                        .iter()
                        .map(|source| source.unwrap_or_default()),
                ),
            );
        parts.push(reused);
    }
    if direct_changed {
        parts.push(pair_projection(
            generation,
            source1,
            source2,
            DIRECT_ROW_KIND,
            "",
            (inputs.direct)(),
        ));
    }
    for &mid_source in inputs.providers {
        if mid_source == source1
            || mid_source == source2
            || (previous.is_some() && !changed_witnesses.contains(&Some(mid_source)))
        {
            continue;
        }
        parts.push(pair_projection(
            generation,
            source1,
            source2,
            BRIDGE_ROW_KIND,
            mid_source,
            (inputs.bridge)(mid_source),
        ));
    }
    let mut parts = parts.into_iter();
    let mut select = parts
        .next()
        .ok_or_else(|| clickhouse::error::Error::custom(EVIDENCE_MISSING))?;
    for part in parts {
        select.union(UnionType::All, part);
    }
    let mut insert = Query::insert();
    insert
        .into_table(Alias::new(EvidenceTable::MappingFixtureEvidence.as_str()))
        .columns(INSERT_COLUMNS);
    insert
        .select_from(select)
        .map_err(|error| clickhouse::error::Error::custom(error.to_string()))?;
    let statement = insert.to_string(MysqlQueryBuilder);
    ClickHouse::add_final_to_from_tables(statement, &[Alias::new(inputs.direct_table)])
        .map_err(|error| clickhouse::error::Error::custom(error.to_string()))
}

fn pair_projection(
    generation: Uuid,
    source1: &str,
    source2: &str,
    row_kind: &str,
    mid_source: &str,
    evidence: SelectStatement,
) -> SelectStatement {
    let mut query = Query::select();
    query
        .expr_as(uuid_expr(generation), EvidenceColumn::Generation)
        .expr_as(Expr::val(row_kind), EvidenceColumn::RowKind)
        .expr_as(Expr::val(source1), EvidenceColumn::Source1)
        .expr_as(Expr::val(source2), EvidenceColumn::Source2)
        .columns(INSERT_COLUMNS[4..14].iter().copied())
        .expr_as(Expr::val(mid_source), EvidenceColumn::MidSource)
        .column((
            Alias::new(EvidenceAlias::CrossSourceEvidence.as_str()),
            Alias::new(EvidenceColumn::IntermediateFixtureId.as_str()),
        ))
        .expr_as(Expr::val(0_i64), EvidenceColumn::LeftCandidateCount)
        .expr_as(Expr::val(0_i64), EvidenceColumn::RightCandidateCount);
    for column in LEG_EVIDENCE_COLUMNS {
        let value = if row_kind == BRIDGE_ROW_KIND && !is_persisted_bridge_detail(column) {
            default_evidence_value(column)
        } else {
            Expr::col((
                Alias::new(EvidenceAlias::CrossSourceEvidence.as_str()),
                column,
            ))
            .into()
        };
        query.expr_as(value, column);
    }
    query
        .from_subquery(
            evidence,
            Alias::new(EvidenceAlias::CrossSourceEvidence.as_str()),
        )
        .to_owned()
}

fn is_persisted_bridge_detail(column: EvidenceColumn) -> bool {
    matches!(
        column,
        EvidenceColumn::LeftStartTime1
            | EvidenceColumn::LeftStartTime2
            | EvidenceColumn::LeftHomeScore1
            | EvidenceColumn::LeftHomeScore2
            | EvidenceColumn::LeftAwayScore1
            | EvidenceColumn::LeftAwayScore2
            | EvidenceColumn::LeftPeriod1
            | EvidenceColumn::LeftPeriod2
            | EvidenceColumn::LeftMatchLevel
            | EvidenceColumn::LeftMatchCondition
            | EvidenceColumn::RightStartTime1
            | EvidenceColumn::RightStartTime2
            | EvidenceColumn::RightHomeScore1
            | EvidenceColumn::RightHomeScore2
            | EvidenceColumn::RightAwayScore1
            | EvidenceColumn::RightAwayScore2
            | EvidenceColumn::RightPeriod1
            | EvidenceColumn::RightPeriod2
            | EvidenceColumn::RightMatchLevel
            | EvidenceColumn::RightMatchCondition
    )
}

fn default_evidence_value(column: EvidenceColumn) -> sea_query::SimpleExpr {
    match column {
        EvidenceColumn::LeftStartTime1
        | EvidenceColumn::LeftStartTime2
        | EvidenceColumn::RightStartTime1
        | EvidenceColumn::RightStartTime2 => Expr::val(0_i64).into(),
        EvidenceColumn::LeftHomeScore1
        | EvidenceColumn::LeftHomeScore2
        | EvidenceColumn::LeftAwayScore1
        | EvidenceColumn::LeftAwayScore2
        | EvidenceColumn::RightHomeScore1
        | EvidenceColumn::RightHomeScore2
        | EvidenceColumn::RightAwayScore1
        | EvidenceColumn::RightAwayScore2 => Expr::val(0_i32).into(),
        _ => Expr::val("").into(),
    }
}

pub async fn direct_row_count(
    revision: &EvidenceRevision,
    client: &clickhouse::Client,
) -> clickhouse::error::Result<u64> {
    let sql = Query::select()
        .expr(sea_query::Func::count(Expr::col(sea_query::Asterisk)))
        .from(Alias::new(EvidenceTable::MappingFixtureEvidence.as_str()))
        .and_where(Expr::col(EvidenceColumn::Generation).eq(uuid_expr(revision.generation)))
        .and_where(Expr::col(EvidenceColumn::Source1).eq(&revision.source1))
        .and_where(Expr::col(EvidenceColumn::Source2).eq(&revision.source2))
        .and_where(Expr::col(EvidenceColumn::RowKind).eq(DIRECT_ROW_KIND))
        .to_string(MysqlQueryBuilder);
    Ok(client.query(&sql).fetch_one::<EvidenceCount>().await?.count)
}

pub async fn complete_revision(
    revision: &mut EvidenceRevision,
    client: &clickhouse::Client,
) -> clickhouse::error::Result<()> {
    let integrity = rectified_integrity(revision, client).await?;
    revision.rectified_rows = integrity.rows;
    revision.rectified_digest =
        multiset_digest(integrity.rows, integrity.hash_sum, integrity.hash_xor);
    Ok(())
}

pub async fn validate_revisions(
    revisions: &[EvidenceRevision],
    expected_pairs: &[(String, String)],
    client: &clickhouse::Client,
) -> clickhouse::error::Result<bool> {
    if !revisions_are_complete(revisions, expected_pairs)
        || !table_exists(client).await?
        || !crate::pipeline::storage::table_exists(client, RECTIFIED_EDGES).await?
    {
        return Ok(false);
    }
    for revision in revisions {
        if !validate_revision(revision, client).await? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub async fn validate_revision(
    revision: &EvidenceRevision,
    client: &clickhouse::Client,
) -> clickhouse::error::Result<bool> {
    if revision.raw_digest.is_empty() || revision.rectified_digest.is_empty() {
        return Ok(false);
    }
    let raw = evidence_integrity_by_names(
        revision.generation,
        &revision.source1,
        &revision.source2,
        client,
    )
    .await?;
    let rectified = rectified_integrity(revision, client).await?;
    Ok(raw.rows == revision.rows
        && raw_digest(&raw) == revision.raw_digest
        && rectified.rows == revision.rectified_rows
        && rectified_digest(&rectified) == revision.rectified_digest)
}

async fn evidence_integrity(
    generation: Uuid,
    source1: &str,
    source2: &str,
    client: &clickhouse::Client,
) -> clickhouse::error::Result<EvidenceIntegrity> {
    evidence_integrity_by_names(generation, source1, source2, client).await
}

async fn evidence_integrity_by_names(
    generation: Uuid,
    source1: &str,
    source2: &str,
    client: &clickhouse::Client,
) -> clickhouse::error::Result<EvidenceIntegrity> {
    let query = multiset_integrity_select(INSERT_COLUMNS.map(|column| Expr::col(column).into()))
        .from(Alias::new(EvidenceTable::MappingFixtureEvidence.as_str()))
        .and_where(Expr::col(EvidenceColumn::Generation).eq(uuid_expr(generation)))
        .and_where(Expr::col(EvidenceColumn::Source1).eq(source1))
        .and_where(Expr::col(EvidenceColumn::Source2).eq(source2))
        .to_string(MysqlQueryBuilder);
    client.query(&query).fetch_one().await.map_err(|error| {
        clickhouse::error::Error::custom(format!("{EVIDENCE_COUNT_ERROR}: {error}"))
    })
}

async fn rectified_integrity(
    revision: &EvidenceRevision,
    client: &clickhouse::Client,
) -> clickhouse::error::Result<EvidenceIntegrity> {
    let columns =
        [GEN, S1, S2, F1, F2, COUNT, FLIP].map(|column| Expr::col(Alias::new(column)).into());
    let query = multiset_integrity_select(columns)
        .from(Alias::new(RECTIFIED_EDGES))
        .and_where(Expr::col(Alias::new(GEN)).eq(uuid_expr(revision.generation)))
        .and_where(Expr::col(Alias::new(S1)).eq(revision.source1.clone()))
        .and_where(Expr::col(Alias::new(S2)).eq(revision.source2.clone()))
        .to_string(MysqlQueryBuilder);
    client.query(&query).fetch_one().await.map_err(|error| {
        clickhouse::error::Error::custom(format!("{EVIDENCE_COUNT_ERROR}: {error}"))
    })
}

fn multiset_integrity_select(
    columns: impl IntoIterator<Item = sea_query::SimpleExpr>,
) -> SelectStatement {
    let row = Expr::tuple(columns);
    let city_hash = Func::cust(Alias::new("cityHash64")).arg(row.clone());
    let sip_hash = Func::cust(Alias::new("sipHash64")).arg(row);
    Query::select()
        .expr_as(
            Func::count(Expr::col(sea_query::Asterisk)),
            Alias::new("rows"),
        )
        .expr_as(Func::sum(city_hash), Alias::new("hash_sum"))
        .expr_as(
            Func::cust(Alias::new("groupBitXor")).arg(sip_hash),
            Alias::new("hash_xor"),
        )
        .to_owned()
}

fn raw_digest(integrity: &EvidenceIntegrity) -> String {
    multiset_digest(integrity.rows, integrity.hash_sum, integrity.hash_xor)
}

fn rectified_digest(integrity: &EvidenceIntegrity) -> String {
    raw_digest(integrity)
}

async fn table_exists(client: &clickhouse::Client) -> clickhouse::error::Result<bool> {
    crate::pipeline::storage::table_exists(client, EvidenceTable::MappingFixtureEvidence.as_str())
        .await
}

fn uuid_expr(value: Uuid) -> sea_query::SimpleExpr {
    Func::cust(Alias::new("toUUID"))
        .arg(Expr::val(value.to_string()))
        .into()
}

/// Keep active evidence and the newest checkpoint for each pair of the
/// selected attempt. A partition without a checkpoint cannot be resumed.
pub async fn prune_attempt_evidence(
    client: &clickhouse::Client,
    attempt: Uuid,
    active: &[EvidenceRevision],
    rectified_stage: &str,
) -> clickhouse::error::Result<usize> {
    const CHECKPOINTS: &str = "mapping_pairwise_checkpoints";
    const SIGNATURE: &str = "signature";
    const STAGE: &str = "stage";
    const DETAILS: &str = "details";
    const CREATED_AT: &str = "created_at";

    #[derive(Row, Deserialize)]
    struct Checkpoint {
        signature: String,
        details: String,
    }
    let prefix = format!("{attempt}/");
    let query = Query::select()
        .columns([SIGNATURE, DETAILS].map(a))
        .from(a(CHECKPOINTS))
        .and_where(Expr::col(a(STAGE)).eq(rectified_stage))
        .and_where(
            Expr::expr(
                Func::cust(a("startsWith"))
                    .args([Expr::col(a(SIGNATURE)).into(), Expr::val(prefix).into()]),
            )
            .eq(true),
        )
        .order_by_expr(
            Func::cust(a("JSONExtractInt"))
                .args([Expr::col(a(DETAILS)).into(), Expr::val(CREATED_AT).into()])
                .into(),
            sea_query::Order::Desc,
        )
        .to_string(MysqlQueryBuilder);

    let mut protected: FxHashSet<_> = active.iter().map(|revision| revision.generation).collect();
    let mut seen = FxHashSet::default();
    for checkpoint in client.query(&query).fetch_all::<Checkpoint>().await? {
        if seen.insert(checkpoint.signature) {
            if let Ok(revision) = serde_json::from_str::<EvidenceRevision>(&checkpoint.details) {
                protected.insert(revision.generation);
            }
        }
    }
    prune_partitions(client, |generation| !protected.contains(&generation)).await
}

/// Rectify a newly written raw pair and record both pieces of its integrity receipt.
pub async fn rectify_revision(
    mut revision: EvidenceRevision,
    reversed_condition: &str,
    click_house: &ClickHouse<'_>,
    client: &clickhouse::Client,
) -> clickhouse::error::Result<EvidenceRevision> {
    const ACCEPTED_LINKS: &str = "accepted_links";
    let started = Instant::now();
    let links = rectified_links(
        revision.generation,
        &revision.source1,
        &revision.source2,
        reversed_condition,
    );
    let projected = Query::select()
        .expr(uuid_expr(revision.generation))
        .expr(Expr::val(&revision.source1))
        .expr(Expr::val(&revision.source2))
        .columns([F1, F2, COUNT, FLIP].map(a))
        .from_subquery(links, a(ACCEPTED_LINKS))
        .to_owned();
    let mut insert = Query::insert();
    insert
        .into_table(a(RECTIFIED_EDGES))
        .columns([GEN, S1, S2, F1, F2, COUNT, FLIP].map(a));
    insert.select_from(projected).map_err(error)?;
    client
        .query(&insert.to_string(MysqlQueryBuilder))
        .execute()
        .await?;
    complete_revision(&mut revision, client).await?;
    click_house.get_logger().log_info(&format!(
        "Rectified {}/{} in {:.1}s",
        revision.source1,
        revision.source2,
        started.elapsed().as_secs_f64(),
    ));
    Ok(revision)
}

#[cfg(test)]
mod cross_source_mapping_query_tests {
    use super::cross_source_mapping_query;
    use sea_query::MysqlQueryBuilder;

    #[test]
    fn builds_bridge_queries_from_direct_mappings_for_each_source_order() {
        for (source1, source2, mid_source) in [
            ("a", "c", "b"),
            ("b", "c", "a"),
            ("a", "b", "c"),
            ("c", "a", "b"),
        ] {
            let sql = cross_source_mapping_query(
                "mapping_fixture_map",
                source1,
                source2,
                mid_source,
                &["start_time1", "home_score1"],
            )
            .to_string(MysqlQueryBuilder)
            .to_ascii_lowercase();

            assert!(sql.contains("mapping_fixture_map"));
            assert!(!sql.contains("mapping_cross_sources_map"));
            assert!(sql.contains("intermediate_fixture_id"));
            assert!(sql.contains("left_start_time1"));
            assert!(sql.contains("right_start_time1"));
            for identity in [
                "country_id",
                "competition_id",
                "home_team_id",
                "away_team_id",
            ] {
                assert!(sql.contains(identity), "query omitted {identity}");
            }
        }
    }
}
