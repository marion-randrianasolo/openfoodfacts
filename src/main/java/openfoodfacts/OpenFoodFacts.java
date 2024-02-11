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
    	
    	Properties props = new Properties();
    	InputStream is = null;
    	try {
    	    is = OpenFoodFacts.class.getClassLoader().getResourceAsStream("application.properties");
    	    props.load(is);
    	} catch (IOException e) {
    	    // Gérer l'exception
    	} finally {
    	    if (is != null) {
    	        try {
    	            is.close();
    	        } catch (IOException e) {
    	            // Gérer l'exception
    	        }
    	    }
    	}

    	String dbPassword = props.getProperty("db.password");
    	
        SparkSession sparkSession = SparkSession.builder().appName("OpenFoodFacts Data").master("local[3]").getOrCreate();
        sparkSession.sparkContext().setLogLevel("WARN");

        String dataFile = "C:\\Users\\mario\\Downloads\\fr.openfoodfacts.org.products.csv";

        Dataset<Row> df = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .option("sep", "\t")
                .option("inferSchema", "true")
                .option("charset", "UTF-8")
                .load(dataFile);

        System.out.println("Nombre de lignes : " + df.count());
        System.out.println("Colonnes : " + Arrays.toString(df.columns()));
        System.out.println("Types de données : " + Arrays.toString(df.dtypes()));

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
           // ).na().drop(new String[]{"product_name", "categories", "nutriscore_score", "energy-kcal_100g", "proteins_100g"});
        		).na().drop();
        
       // filteredDataset.show();
        
        System.out.println("Filtered Dataset Rows: " + filteredDataset.count());

        WindowSpec window = Window.orderBy(col("product_name"));
        Dataset<Row> indexedDataset = filteredDataset.withColumn("product_id", functions.row_number().over(window));

     //  indexedDataset.show();
        
        String dataReqFile = "C:\\Users\\mario\\Downloads\\diet_requirements.csv";

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
    //    ketoFriendlyProducts.show();

        // Filter DataFrame on products for LowCarb 
        Dataset<Row> lowCarbFriendlyProducts = indexedDataset.filter(col("carbohydrates_100g").lt(maxCarbsLowCarb));

        System.out.println("LowCarb Friendly Products:");
     //   lowCarbFriendlyProducts.show();

        
     // Chargez les données des utilisateurs
        String userFile = "C:\\Users\\mario\\Downloads\\user_dietary_preferences.csv";
        Dataset<Row> userData = sparkSession.read()
            .format("csv")
            .option("header", "true")
            .load(userFile);
        
     // Configuration des propriétés de connexion à la base de données
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", dbPassword); // Assurez-vous de sécuriser votre mot de passe réel
  
     //   String jdbcUrl = "jdbc:mysql://b4cn7kipysfb3tfgheh3-mysql.services.clever-cloud.com:3306/b4cn7kipysfb3tfgheh3?useSSL=false&characterEncoding=UTF-8";

        String jdbcUrl = "jdbc:mysql://localhost:3306/openfoodfacts_test?useSSL=false&characterEncoding=UTF-8";

        
        // Pour les produits Keto
        writeToDatabase(ketoFriendlyProducts, "products", jdbcUrl, connectionProperties);

        // Pour les produits LowCarb
        writeToDatabase(lowCarbFriendlyProducts, "products", jdbcUrl, connectionProperties);

        // pour insérer des utilisateurs
        writeToDatabase(userData, "users", jdbcUrl, connectionProperties);
        

        // Itérez sur chaque utilisateur pour générer un menu personnalisé
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
                
        Dataset<Row> dbData = sparkSession.read()
        	    .jdbc(jdbcUrl, "daily_menus", connectionProperties);
        	dbData.show();

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

      //  dailyMenu.show();

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
       // weeklyMenu.show();

    }


    
    private static void writeToDatabase(Dataset<Row> dataframe, String tableName, String jdbcUrl, Properties connectionProperties) {
    	dataframe.write()
        	.mode(SaveMode.Ignore) // Ignore records that cause primary key conflicts
        	.jdbc(jdbcUrl, tableName, connectionProperties);
        System.out.println("Data written to table: " + tableName);

    }
    
    private static final AtomicInteger counter = new AtomicInteger(50); // start from 1

    private static int generateUniqueMenuId() {
        // Generate a sequential unique ID
        return counter.getAndIncrement();
    }

}
