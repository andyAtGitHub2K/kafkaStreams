package ahbun.chapter9;


import ahbun.model.StockTransaction;
import ahbun.util.DataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.*;


public class H2DbClient {
    private enum ColumnIndex {
        SMBL, SCTR, INDSTY, SHRS, SHRPRC, CSTMRID, TXNTS
    }

    private static Logger logger = LoggerFactory.getLogger(H2DbClient.class);
    private static final String DRIVER_CLASS_NAME="org.h2.Driver";
    private static final String DB_URL = "jdbc:h2:tcp://localhost:9989/~/findata";
    private static final String USER = "sa";
    private static final String PW = "";
    private static Connection connection;

    private static String createStatement = "create table if not exists transactions(TXN_ID bigint auto_increment," +
            "SMBL varchar(255), SCTR varchar(255), INDSTY varchar(255), " +
            "SHRS integer, SHRPRC double, CSTMRID varchar(255), TXNTS TIMESTAMP ) ";

    private static String insertStatement = "insert into transactions (SMBL, SCTR, INDSTY, SHRS, SHRPRC, CSTMRID, TXNTS)" +
            " values (?, ?, ?, ?, ?, ?, ?)";

    // data generation
    private static final String[] INDUSTRY_LIST = {"food", "toy", "medical", "movies"};
    private static final int CUSTOMER_SIZE = 5;
    private static final byte TRUE = 0x01;
    private static final byte FALSE = 0x0;

    private static final ZoneId ZONE_ID = ZoneId.of("America/Los_Angeles");
    private static ExecutorService executorService = Executors.newCachedThreadPool();

    /***
     * Establish SQL connection to periodically insert transaction records to relational database
     * in DB server.
     *
     * @param argv number of records and batch interval
     */
    public static void main(String[] argv) {
        try {
            Config config = new Config(argv);
            logger.info(Config.getInfo());
            establishDBConnectionUsing(config);
            Future<Integer> totalRecordsInserted = insertRecords(config);
            logger.info("Inserted " + totalRecordsInserted.get() + " records.");
        } catch (Exception ex) {
            usage();
            logger.error(ex.getMessage());
        } finally {
            closeConnection();
            logger.info("insertion is completed");
        }
    }

    private static void establishDBConnectionUsing(Config config) throws SQLException, ClassNotFoundException {
        // check for access driver and create connection

        Class.forName(DRIVER_CLASS_NAME);
        connection = DriverManager.getConnection(DB_URL, USER, PW);
        connection.setAutoCommit(false);
        connection.prepareStatement(createStatement).execute();
    }

    /***
     *
     * @param config
     */
    private static Future<Integer> insertRecords(Config config) {
        Callable<Integer> insertThread = () -> {
            logger.info("starting insert record size " + Config.getBatchSize() + " for " + Config.getIteration() + " iteration");
            establishDBConnectionUsing(config);

            // generate tx records int iteration, int customerSize, String[] industryList
            List<StockTransaction> stockTransactionList =
                    DataGenerator.makeStockTx(Config.getBatchSize(), CUSTOMER_SIZE, INDUSTRY_LIST);
            for (int i = 0; i < Config.getIteration(); i++) {
                executeInsertTask(stockTransactionList);
                Thread.sleep(Config.getBatchInterval());
                logger.info("insert batch count: " + i);
            }

            return Config.getIteration() * stockTransactionList.size();
        };

        return executorService.submit(insertThread);
    }

    /***
     * update insert values and execute insertion statement
     * @param stockTransactionList
     */
    private static void executeInsertTask(List<StockTransaction> stockTransactionList) {
        PreparedStatement preparedStatement = null;

        try {
            preparedStatement = connection.prepareStatement(insertStatement, Statement.RETURN_GENERATED_KEYS);
            for (StockTransaction stockTransaction: stockTransactionList) {
                int column = ColumnIndex.SMBL.ordinal();
                preparedStatement.setString(ColumnIndex.SMBL.ordinal() + 1, stockTransaction.getSymbol());
                preparedStatement.setString(ColumnIndex.SCTR.ordinal() + 1, stockTransaction.getSector());
                preparedStatement.setString(ColumnIndex.INDSTY.ordinal() + 1, stockTransaction.getIndustry());
                preparedStatement.setInt(ColumnIndex.SHRS.ordinal() + 1, stockTransaction.getShares());
                preparedStatement.setDouble(ColumnIndex.SHRPRC.ordinal() + 1, stockTransaction.getSharePrice());
                preparedStatement.setString(ColumnIndex.CSTMRID.ordinal() + 1, stockTransaction.getCustomerId());
                Date date = stockTransaction.getTransactionTimestamp();
                preparedStatement.setTimestamp(ColumnIndex.TXNTS.ordinal() + 1, new java.sql.Timestamp(date.getTime()));
                preparedStatement.addBatch();
            }

            preparedStatement.executeBatch();
            ResultSet resultSet = preparedStatement.getGeneratedKeys();
            if (resultSet.next()) {
                logger.info("first result record id: " + resultSet.getInt(1));
            }

            connection.commit();
        } catch (SQLException ex) {
            logger.error(ex.getMessage());
        } finally {
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
            } catch (SQLException ex) {
                logger.error(ex.getMessage());
            }
        }
    }

    private static void closeConnection() {
        try {
            if (H2DbClient.connection != null) {
                H2DbClient.connection.close();
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    private static void usage() {
        System.out.println("Usage: H2DbClient iteration batch_size batch_interval_ms");
        System.out.println("default: iteration = 1, batch_size = 1,  batch_interval_ms = 0");
    }

    private static class Config {
        private static final int DEFAULT_ITERATION = 1;
        private static final int DEFAULT_BATCH_SIZE = 1;
        private static final long DEFAULT_BATCH_INTERVAL_MS = 0;
        private static int iteration;
        private static int batchSize;
        private static long batchInterval;

        public Config(String[] params) {
            if (!parseParams(params)) {
                iteration = DEFAULT_ITERATION;
                batchSize = DEFAULT_BATCH_SIZE;
                batchInterval = DEFAULT_BATCH_INTERVAL_MS;
            }
        }

        public static int getIteration() {
            return iteration;
        }

        public static int getBatchSize() {
            return batchSize;
        }

        public static long getBatchInterval() {
            return batchInterval;
        }

        public static String getInfo() {
            return String.format("iteration: %d, batch size: %d, interval: %d",
                    getIteration(), getBatchSize(), getBatchInterval());

        }
        private boolean parseParams(String[] params) {
            if (params.length == 3) {
                iteration  = OptionalInt.of(Integer.parseInt(params[0])).orElse(DEFAULT_ITERATION);
                batchSize  = OptionalInt.of(Integer.parseInt(params[1])).orElse(DEFAULT_BATCH_SIZE);
                batchInterval = OptionalLong.of(Long.parseLong(params[2])).orElse(DEFAULT_BATCH_INTERVAL_MS);
                return true;
            } else {
                return false;
            }
        }

    }
}
