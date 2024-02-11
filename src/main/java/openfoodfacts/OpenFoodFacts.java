package openfoodfacts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.io.InputStream;

public class OpenFoodFacts {

    public static void main(String[] args) {
    	
        // Initialize a Properties object to load configuration settings
    	Properties props = new Properties();
    	InputStream is = null;
    	try {
            // Load the application.properties file
    	    is = OpenFoodFacts.class.getClassLoader().getResourceAsStream("application.properties");
    	    props.load(is);
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    	    if (is != null) {
    	        try {
    	            is.close();
    	        } catch (IOException e) {
    	            e.printStackTrace();
    	        }
    	    }
    	}

        // Retrieve the database password from the loaded properties
    	String dbPassword = props.getProperty("db.password");
    	
        // Create a SparkSession with a specified app name and run it locally using 3 worker threads
        SparkSession sparkSession = SparkSession.builder().appName("OpenFoodFacts Data").master("local[3]").getOrCreate();
        // Set log level to WARN to reduce the amount of log output
        sparkSession.sparkContext().setLogLevel("WARN");
        
        // Database connection properties
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", dbPassword);
  
        // JDBC URL for connecting to the database
        String jdbcUrl = "jdbc:mysql://localhost:3306/openfoodfacts?useSSL=false&characterEncoding=UTF-8";
        // Initialize the counter for daily menus based on existing data in the database
        initializeCounterForDailyMenus(sparkSession, jdbcUrl, connectionProperties);

        // Path to the CSV file containing openfoodfacts data
        String dataFile = "C:\\Users\\mario\\Downloads\\fr.openfoodfacts.org.products.csv";

        // Load the CSV file into a DataFrame
        Dataset<Row> df = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .option("sep", "\t")
                .option("inferSchema", "true")
                .option("charset", "UTF-8")
                .load(dataFile);

        // Print out the number of rows and the schema of the loaded DataFrame
        System.out.println("Nombre de lignes : " + df.count());
        System.out.println("Colonnes : " + Arrays.toString(df.columns()));
        System.out.println("Types de données : " + Arrays.toString(df.dtypes()));

        // Filter the DataFrame based on certain columns and drop any rows with missing values
        Dataset<Row> filteredDataset = df.select(
                df.col("product_name"),
                df.col("categories"),
                df.col("labels"),
                df.col("ingredients_text"),
                df.col("nutriscore_score").cast("float"),
                df.col("energy-kcal_100g").cast("float"),
                df.col("fat_100g").cast("float"),
                df.col("saturated-fat_100g").cast("float"),
                df.col("carbohydrates_100g").cast("float"),
                df.col("sugars_100g").cast("float"),
                df.col("fiber_100g").cast("float"),
                df.col("proteins_100g").cast("float"),
                df.col("salt_100g").cast("float")
        		).na().drop();           
        System.out.println("Filtered Dataset Rows: " + filteredDataset.count());

        // Define a WindowSpec to order rows by product name
        WindowSpec window = Window.orderBy(col("product_name"));
        
        // Add a 'product_id' column to the DataFrame by assigning a unique ID to each row based on the window specification
        Dataset<Row> indexedDataset = filteredDataset.withColumn("product_id", functions.row_number().over(window));
        
        // Path to the CSV file containing diet requirements
        String dataReqFile = "C:\\Users\\mario\\Downloads\\diet_requirements.csv";
 
        // Load dietary requirements into a DataFrame
        Dataset<Row> dietRequirements = sparkSession.read()
            .format("csv")
            .option("header", "true")
            .load(dataReqFile);

        // Get requirements for Keto Diet
        Row ketoRequirements = dietRequirements.filter(col("DietType").equalTo("Keto")).collectAsList().get(0);
        String maxCarbsKeto = ketoRequirements.getAs("MaxCarbohydrates_100g");
        String minFatKeto = "15";

        // Get requirements for Lowcarb Diet
        Row lowCarbRequirements = dietRequirements.filter(col("DietType").equalTo("LowCarb")).collectAsList().get(0);
        String maxCarbsLowCarb = lowCarbRequirements.getAs("MaxCarbohydrates_100g");

        // Filter DataFrame on products for Keto 
        Dataset<Row> ketoFriendlyProducts = indexedDataset.filter(col("carbohydrates_100g").lt(maxCarbsKeto)
            .and(col("fat_100g").gt(minFatKeto)));
        System.out.println("Keto Friendly Products:");
        
        // Filter DataFrame on products for LowCarb 
        Dataset<Row> lowCarbFriendlyProducts = indexedDataset.filter(col("carbohydrates_100g").lt(maxCarbsLowCarb));
        System.out.println("LowCarb Friendly Products:");

        
        // Load CSV file containing user data
        String userFile = "C:\\Users\\mario\\Downloads\\user_dietary_preferences.csv";
        Dataset<Row> userData = sparkSession.read()
            .format("csv")
            .option("header", "true")
            .load(userFile);
        
     

        
        // Write the filtered keto products in database
        writeToDatabase(ketoFriendlyProducts, "products", jdbcUrl, connectionProperties);

        // Write the filtered lowcarb products in database
        writeToDatabase(lowCarbFriendlyProducts, "products", jdbcUrl, connectionProperties);

        // Write user data from dataframe in database
        writeToDatabase(userData, "users", jdbcUrl, connectionProperties);
        

        // Generate and write personalized weekly menus for each user.
        userData.collectAsList().forEach(user -> {
        	Integer userId = Integer.parseInt(user.getAs("user_id"));
            String userDietType = user.getAs("diet_type");
            Dataset<Row> userFriendlyProducts;

            if ("Keto".equals(userDietType)) {
                userFriendlyProducts = ketoFriendlyProducts;
            } else if ("LowCarb".equals(userDietType)) {
                userFriendlyProducts = lowCarbFriendlyProducts;
            } else {
                userFriendlyProducts = indexedDataset;
            }
            
            generateWeeklyMenu(sparkSession, userFriendlyProducts, userId, jdbcUrl, connectionProperties);
        });
                
        // Display content of "daily_menus" table.
        Dataset<Row> dbData = sparkSession.read()
        	    .jdbc(jdbcUrl, "daily_menus", connectionProperties);
        	dbData.show();

       	// End of session.
        System.out.println("--- Session over ---");
        sparkSession.stop();

    }

    private static Dataset<Row> generateDailyMenu(SparkSession spark, Dataset<Row> userFriendlyProducts, int menuId, Integer userId, String day) {
        // Select random products for the daily menu
        Dataset<Row> dailyMenu = userFriendlyProducts.sample(false, 0.1).limit(5);

        // Convert the selected products into a single string of product IDs
        String productIds = dailyMenu.select("product_id")
                                     .as(Encoders.STRING())
                                     .collectAsList()
                                     .stream()
                                     .collect(Collectors.joining(","));

        // Create a Row with the menu information
        Row menuRow = RowFactory.create(menuId, userId, day, productIds);

        // Define the schema of the menu DataFrame
        StructType menuSchema = new StructType(new StructField[]{
            new StructField("menu_id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("user_id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("day_of_week", DataTypes.StringType, false, Metadata.empty()),
            new StructField("product_ids", DataTypes.StringType, false, Metadata.empty())
        });

        dailyMenu.show();

        // Create a DataFrame with the menu information
        return spark.createDataFrame(Arrays.asList(menuRow), menuSchema);
        
    }


    private static void generateWeeklyMenu(SparkSession spark, Dataset<Row> userFriendlyProducts, Integer userId, String jdbcUrl, Properties connectionProperties) {
        String[] daysOfWeek = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};
        List<Integer> weeklyMenuIds = new ArrayList<>();
        
        for (String day : daysOfWeek) {
            // Generate a unique menu_id for each day
            int menuId = generateUniqueMenuId();
            
            // Generate the daily menu DataFrame with the generated menu_id
            Dataset<Row> dailyMenu = generateDailyMenu(spark, userFriendlyProducts, menuId, userId, day);

            // Write the daily menu to the database
            writeToDatabase(dailyMenu, "daily_menus", jdbcUrl, connectionProperties);

            // Save the generated menu_id for weekly aggregation
            weeklyMenuIds.add(menuId);
        }

        // Convert the list of menu_ids into a comma-separated string
        String menuIdsString = weeklyMenuIds.stream()
                                            .map(Object::toString)
                                            .collect(Collectors.joining(","));

        // Create a Row for the weekly menu
        Row weeklyMenuRow = RowFactory.create(userId, menuIdsString);
        StructType weeklyMenuSchema = new StructType(new StructField[]{
            new StructField("user_id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("menu_ids", DataTypes.StringType, false, Metadata.empty())
        });

        // Create a DataFrame with the weekly menu information
        Dataset<Row> weeklyMenu = spark.createDataFrame(Arrays.asList(weeklyMenuRow), weeklyMenuSchema);
        
        // Write the weekly menu DataFrame to the database
        writeToDatabase(weeklyMenu, "weekly_menus", jdbcUrl, connectionProperties);
        weeklyMenu.show();

    }


    // 	Write DataFrame to database table.
    private static void writeToDatabase(Dataset<Row> dataframe, String tableName, String jdbcUrl, Properties connectionProperties) {
        try {
            dataframe.write()
                .mode(SaveMode.Append)
                .jdbc(jdbcUrl, tableName, connectionProperties);
            System.out.println("Data written to table: " + tableName);
        } catch (Exception e) {
            System.out.println("Exception while writing to table: " + tableName);
            e.printStackTrace();
        }
    }
    
    // Initialize ID counter based on the highest ID in the 'daily_menus' table.
    private static void initializeCounterForDailyMenus(SparkSession spark, String jdbcUrl, Properties connectionProperties) {
        Dataset<Row> maxIdResult = spark.read().jdbc(jdbcUrl, "daily_menus", connectionProperties)
                .agg(functions.max(col("menu_id")).alias("max_id"));
        Integer maxId = maxIdResult.collectAsList().get(0).getAs("max_id");
        if (maxId != null) {
            counter.set(maxId + 1); // Set counter to max_id + 1.
        } else {
            counter.set(1);  // If no existing IDs, start counter at 1.
        }
    }
    
    // Counter to generate unique IDs, starting from 1.
    private static final AtomicInteger counter = new AtomicInteger(1); // start from 1

    // Generates and returns a unique ID.
    private static int generateUniqueMenuId() {
        return counter.getAndIncrement(); // Increment and return counter.
    }

}
