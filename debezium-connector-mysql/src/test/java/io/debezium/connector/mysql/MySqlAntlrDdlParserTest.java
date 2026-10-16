/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.connector.binlog.BinlogAntlrDdlParserTest;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.connector.mysql.charset.MySqlCharsetRegistry;
import io.debezium.connector.mysql.jdbc.MySqlDefaultValueConverter;
import io.debezium.connector.mysql.jdbc.MySqlValueConverters;
import io.debezium.connector.mysql.util.MySqlValueConvertersFactory;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.SimpleDdlParserListener;

/**
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class MySqlAntlrDdlParserTest
        extends BinlogAntlrDdlParserTest<MySqlValueConverters, MySqlDefaultValueConverter, MySqlAntlrDdlParser>
        implements MySqlCommon {
    @Override
    protected MySqlAntlrDdlParser getParser(SimpleDdlParserListener listener) {
        return new MySqlDdlParserWithSimpleTestListener(listener);
    }

    @Override
    protected MySqlAntlrDdlParser getParser(SimpleDdlParserListener listener, boolean includeViews) {
        return new MySqlDdlParserWithSimpleTestListener(listener, includeViews);
    }

    @Override
    protected MySqlAntlrDdlParser getParser(SimpleDdlParserListener listener, TableFilter tableFilter) {
        return new MySqlDdlParserWithSimpleTestListener(listener, tableFilter);
    }

    @Override
    protected MySqlAntlrDdlParser getParser(SimpleDdlParserListener listener, boolean includeViews,
                                            boolean includeComments) {
        return new MySqlDdlParserWithSimpleTestListener(listener, includeViews, includeComments);
    }

    @Override
    protected MySqlValueConverters getValueConverters() {
        return new MySqlValueConvertersFactory().create(
                RelationalDatabaseConnectorConfig.DecimalHandlingMode
                        .parse(JdbcValueConverters.DecimalMode.DOUBLE.name()),
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS,
                BinlogConnectorConfig.BigIntUnsignedHandlingMode
                        .parse(JdbcValueConverters.BigIntUnsignedMode.PRECISE.name()),
                BinaryHandlingMode.BYTES,
                EventConvertingFailureHandlingMode.WARN);
    }

    @Override
    protected MySqlDefaultValueConverter getDefaultValueConverters(MySqlValueConverters valueConverters) {
        return new MySqlDefaultValueConverter(valueConverters);
    }

    @Override
    protected List<String> extractEnumAndSetOptions(List<String> enumValues) {
        return MySqlAntlrDdlParser.extractEnumAndSetOptions(enumValues);
    }

    @Test
    public void shouldParseTdsqlDistributedByCreateTable() {
        var listener = new SimpleDdlParserListener();
        var parser = new MySqlDdlParserWithSimpleTestListener(listener);
        var tables = new Tables();

        var ddl = "CREATE TABLE tb_sub_ev (\n"
                + "  id int NOT NULL,\n"
                + "  purchased date NOT NULL,\n"
                + "  PRIMARY KEY (id,purchased)\n"
                + ") ENGINE=InnoDB\n"
                + "PARTITION BY RANGE (YEAR(purchased))\n"
                + "    SUBPARTITION BY HASH (TO_DAYS(purchased))\n"
                + "(PARTITION p0 VALUES LESS THAN (1990)\n"
                + "       (SUBPARTITION s0 ENGINE = InnoDB,\n"
                + "       SUBPARTITION s1 ENGINE = InnoDB),\n"
                + " PARTITION p1 VALUES LESS THAN (2000)\n"
                + "       (SUBPARTITION s2 ENGINE = InnoDB,\n"
                + "       SUBPARTITION s3 ENGINE = InnoDB))\n"
                + "TDSQL_DISTRIBUTED BY RANGE(id) (s1 values less than ('100'),s2 values less than ('1000'));";

        parser.parse(ddl, tables);

        assertThat(parser.getParsingExceptionsFromWalker()).isEmpty();
        var table = tables.forTable(new TableId(null, null, "tb_sub_ev"));
        assertThat(table).isNotNull();
        assertThat(table.retrieveColumnNames()).containsExactly("id", "purchased");
        assertThat(table.primaryKeyColumnNames()).containsExactly("id", "purchased");
    }

    @Test
    public void shouldParseTdsqlDistributedByCreateTable2() {
        var listener = new SimpleDdlParserListener();
        var parser = new MySqlDdlParserWithSimpleTestListener(listener);
        var tables = new Tables();

        var ddl = "/*1:4954574-1789439700-390*/CREATE TABLE IF NOT EXISTS tqw_wallet_tran_detail (\n"
                + "ecny_cust_no VARCHAR(34) NOT NULL ,\n"
                + "voucher_no VARCHAR(32) NOT NULL ,\n"
                + "system_seq_no VARCHAR(40) NOT NULL ,\n"
                + "tran_dt VARCHAR(8) NOT NULL ,\n"
                + "tran_tm VARCHAR(6) NOT NULL ,\n"
                + "wallet_id VARCHAR(20) NOT NULL ,\n"
                + "wallet_nm VARCHAR(180) ,\n"
                + "real_nm VARCHAR(180) ,\n"
                + "enc_phone VARCHAR(128) ,\n"
                + "tran_wallet_id VARCHAR(68) NOT NULL ,\n"
                + "gl_seq_no VARCHAR(30) NOT NULL ,\n"
                + "chnl_seq_no VARCHAR(40) NOT NULL ,\n"
                + "chnl_no VARCHAR(10) NOT NULL ,\n"
                + "chnl_dt VARCHAR(8) NOT NULL,\n"
                + "PRIMARY KEY (ecny_cust_no, voucher_no, tran_dt, tran_wallet_id),\n"
                + "INDEX tqw_wallet_tran_detail_idx1 (chnl_dt, chnl_seq_no),\n"
                + "INDEX tqw_wallet_tran_detail_idx2 (wallet_id)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n"
                + "PARTITION BY LIST (murmurHashCodeAndMod(`ecny_cust_no`,16))\n"
                + "SUBPARTITION BY RANGE (tdsql_month(`tran_dt`)) SUBPARTITION TEMPLATE (\n"
                + "SUBPARTITION pd_203004 VALUES LESS THAN (203005),\n"
                + "SUBPARTITION pd_203005 VALUES LESS THAN (203006),\n"
                + "SUBPARTITION pd_203006 VALUES LESS THAN (203007),\n"
                + "SUBPARTITION pd_203007 VALUES LESS THAN (203008),\n"
                + "SUBPARTITION pd_203008 VALUES LESS THAN (203009),\n"
                + "SUBPARTITION pd_203009 VALUES LESS THAN (203010),\n"
                + "SUBPARTITION pd_203010 VALUES LESS THAN (203011),\n"
                + "SUBPARTITION pd_203011 VALUES LESS THAN (203012),\n"
                + "SUBPARTITION pd_203012 VALUES LESS THAN (203101)\n"
                + ") (PARTITION p8 VALUES IN (8),PARTITION p9 VALUES IN (9),PARTITION p10 VALUES IN (10),PARTITION p11 VALUES IN (11))";

        parser.parse(ddl, tables);

        assertThat(parser.getParsingExceptionsFromWalker()).isEmpty();
        var table = tables.forTable(new TableId(null, null, "tqw_wallet_tran_detail"));
        assertThat(table).isNotNull();
        assertThat(table.retrieveColumnNames()).containsExactly(
                "ecny_cust_no", "voucher_no", "system_seq_no", "tran_dt", "tran_tm",
                "wallet_id", "wallet_nm", "real_nm", "enc_phone", "tran_wallet_id",
                "gl_seq_no", "chnl_seq_no", "chnl_no", "chnl_dt");
        assertThat(table.primaryKeyColumnNames()).containsExactly("ecny_cust_no", "voucher_no", "tran_dt", "tran_wallet_id");
    }

    @Test
    public void shouldParseTdsqlDistributedByHashCreateTable() {
        var listener = new SimpleDdlParserListener();
        var parser = new MySqlDdlParserWithSimpleTestListener(listener);
        var tables = new Tables();

        var ddl = "create table `my_table`(\n"
                + "    `col1` varchar(50) COLLATE utf8mb4_bin NOT NULL COMMENT '1',\n"
                + "    `col2` varchar(50) COLLATE utf8mb4_bin NOT NULL COMMENT '2',\n"
                + "    primary key (`col1`, `col2`)\n"
                + ") engine=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='my table'\n"
                + "TDSQL_DISTRIBUTED BY HASH(`col1`)";

        parser.parse(ddl, tables);

        assertThat(parser.getParsingExceptionsFromWalker()).isEmpty();
        var table = tables.forTable(new TableId(null, null, "my_table"));
        assertThat(table).isNotNull();
        assertThat(table.retrieveColumnNames()).containsExactly("col1", "col2");
        assertThat(table.primaryKeyColumnNames()).containsExactly("col1", "col2");
    }

    @Test
    public void shouldParseTdsqlDistributedByHashWithTdsqlPartitionCreateTable() {
        var listener = new SimpleDdlParserListener();
        var parser = new MySqlDdlParserWithSimpleTestListener(listener);
        var tables = new Tables();

        var ddl = "create table `my_table`(\n"
                + "    `col1` varchar(50) COLLATE utf8mb4_bin NOT NULL COMMENT '1',\n"
                + "    `col2` varchar(50) COLLATE utf8mb4_bin NOT NULL COMMENT '2',\n"
                + "    `date_col` date not null comment '3',\n"
                + "    primary key (`col1`, `col2`)\n"
                + ") engine=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='my table'\n"
                + "TDSQL_DISTRIBUTED BY HASH(`col1`)\n"
                + "TDSQL_PARTITION BY RANGE (tdsql_month(`date_col`))\n"
                + "(PARTITION P202501 VALUES LESS THAN (202501) ENGINE = InnoDB,\n"
                + " PARTITION P202502 VALUES LESS THAN (202502) ENGINE = InnoDB,\n"
                + " PARTITION P202503 VALUES LESS THAN (202503) ENGINE = InnoDB)";

        parser.parse(ddl, tables);

        assertThat(parser.getParsingExceptionsFromWalker()).isEmpty();
        var table = tables.forTable(new TableId(null, null, "my_table"));
        assertThat(table).isNotNull();
        assertThat(table.retrieveColumnNames()).containsExactly("col1", "col2", "date_col");
        assertThat(table.primaryKeyColumnNames()).containsExactly("col1", "col2");
    }

    public static class MySqlDdlParserWithSimpleTestListener extends MySqlAntlrDdlParser {
        MySqlDdlParserWithSimpleTestListener(DdlChanges changesListener) {
            this(changesListener, false);
        }

        MySqlDdlParserWithSimpleTestListener(DdlChanges changesListener, TableFilter tableFilter) {
            this(changesListener, false, false, tableFilter);
        }

        MySqlDdlParserWithSimpleTestListener(DdlChanges changesListener, boolean includeViews) {
            this(changesListener, includeViews, false, TableFilter.includeAll());
        }

        MySqlDdlParserWithSimpleTestListener(DdlChanges changesListener, boolean includeViews,
                                             boolean includeComments) {
            this(changesListener, includeViews, includeComments, TableFilter.includeAll());
        }

        private MySqlDdlParserWithSimpleTestListener(DdlChanges changesListener, boolean includeViews,
                                                     boolean includeComments, TableFilter tableFilter) {
            super(false, includeViews, includeComments, tableFilter, new MySqlCharsetRegistry());
            this.ddlChanges = changesListener;
        }
    }
}
