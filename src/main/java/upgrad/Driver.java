package upgrad;

import com.mongodb.client.*;
import org.bson.Document;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Driver {

    /**
     * Driver class main method
     *
     * @param args
     * @throws SQLException
     */
    public static void main(String[] args) {
        // MySql credentials
        String url = "jdbc:mysql://pgc-sd-bigdata.cyaielc9bmnf.us-east-1.rds.amazonaws.com:3306/pgcdata";
        String user = "student";
        String password = "STUDENT123";

        // MongoDB Configurations
        String mongoHost = "mongodb://52.70.233.142";
        int mongoPort = 27017;
        String dbName = "upgradAssignment";
        String collectionName = "products";

        // Connection Default Value Initialization
        Connection sqlConnection = null;
        MongoClient mongoClient = null;

        try {
            // Creating database connections
            sqlConnection = DriverManager.getConnection(url, user, password);
            Statement statement = sqlConnection.createStatement();
            String sql = "select * from mobiles;";
            ResultSet resultMobiles = statement.executeQuery(sql);

            /* Creating mongoDB connections and accessing product collection
            Below mongoClient connections is created by passing the mongoHost ip i.e. 00.00.000.00
            and mongoPort which help us to access the mongoDB source which is hosted over AWS EC2 server
             */
            mongoClient = MongoClients.create(mongoHost + ":" + mongoPort);
            MongoDatabase database = mongoClient.getDatabase(dbName);
            MongoCollection<Document> collection = database.getCollection(collectionName);

            // List to append the document
            List<Document> documents = new ArrayList<Document>();

            // Import data into MongoDb
            // Appending Document type list to insert mobile list into Products collection
            while (resultMobiles.next()) {
                documents.add(new Document("ProductId", resultMobiles.getString("ProductId"))
                        .append("Category", "Mobile")
                        .append("Title", resultMobiles.getString("Title"))
                        .append("Manufacturer", resultMobiles.getString("Manufacturer"))
                        .append("NetworkTechnology", resultMobiles.getString("NetworkTechnology"))
                        .append("Dimensions", resultMobiles.getString("Dimensions"))
                        .append("Weight", resultMobiles.getString("Weight"))
                        .append("Display", resultMobiles.getString("Display"))
                        .append("Bluetooth", resultMobiles.getString("Bluetooth"))
                        .append("Sensors", resultMobiles.getString("Sensors"))
                        .append("OS", resultMobiles.getString("OS"))
                        .append("Chipset", resultMobiles.getString("Chipset"))
                        .append("CPU", resultMobiles.getString("CPU"))
                        .append("GPU", resultMobiles.getString("GPU"))
                        .append("Memory", resultMobiles.getString("Memory"))
                        .append("Camera", resultMobiles.getString("Camera"))
                        .append("Battery", resultMobiles.getString("Battery")));
            }

            // Appending Document type list to insert cameras into products collections
            sql = "select * from cameras;";
            ResultSet resultCameras = statement.executeQuery(sql);
            while (resultCameras.next()) {
                documents.add(new Document("ProductId", resultCameras.getString("ProductId"))
                        .append("Category", "Camera")
                        .append("Title", resultCameras.getString("Title"))
                        .append("Manufacturer", resultCameras.getString("Manufacturer"))
                        .append("EffectivePixels", resultCameras.getString("EffectivePixels"))
                        .append("Zoom", resultCameras.getString("Zoom"))
                        .append("Dimension", resultCameras.getString("Dimension"))
                        .append("Weight", resultCameras.getString("Weight"))
                        .append("VideoResolution", resultCameras.getString("VideoResolution"))
                        .append("ShutterSpeed", resultCameras.getString("ShutterSpeed"))
                        .append("Battery", resultCameras.getString("Battery")));
            }

            // Appending Document type list to insert headphone into products collections
            sql = "select * from headphones;";
            ResultSet resultHeadphones = statement.executeQuery(sql);
            while (resultHeadphones.next()) {
                documents.add(new Document("ProductId", resultHeadphones.getString("ProductId"))
                        .append("Category", "Headphone")
                        .append("Title", resultHeadphones.getString("Title"))
                        .append("Manufacturer", resultHeadphones.getString("Manufacturer"))
                        .append("HeadPhoneType", resultHeadphones.getString("HeadPhoneType"))
                        .append("Battery", resultHeadphones.getString("Battery"))
                        .append("Warranty", resultHeadphones.getString("Warranty"))
                        .append("ConnectorType", resultHeadphones.getString("ConnectorType"))
                        .append("WithMicrophone", resultHeadphones.getString("WithMicrophone"))
                        .append("ItemWeight", resultHeadphones.getString("ItemWeight"))
                        .append("Color", resultHeadphones.getString("Color"))
                        .append("AdditionalFeatures", resultHeadphones.getString("AdditionalFeatures")));
            }

            /* Inserting all the products in one go by using insertMany function of MongoCollection,
            this will insert the complete collection of Mobile, Cameras and Headphone into Product collection
            in upgradAssignment database
             */
            collection.insertMany(documents);

            // List all products in the inventory
            CRUDHelper.displayAllProducts(collection);

            // Display top 5 Mobiles
            CRUDHelper.displayTop5Mobiles(collection);

            // Display products ordered by their categories in Descending Order Without autogenerated Id
            CRUDHelper.displayCategoryOrderedProductsDescending(collection);

            // Display product count in each category
            CRUDHelper.displayProductCountByCategory(collection);

            // Display wired headphones
            CRUDHelper.displayWiredHeadphones(collection);

            /*
               below code help us to drop the collection to avoid the multiple insertion in the Collection
               as above code doesn't checks for duplications while inserting or appending the product list
                collection.drop();
             */
            collection.drop();
        } catch (Exception ex) {
            System.out.println("Got Exception.");
            ex.printStackTrace();
        } finally {
            // Close Connections
            /*
            below try catch is used due to sqlConnection class  will through unhandled exception so to capture this
            implemented another try catch in finally block which will help us to handle the exception which can be thrown
            due to the sqlConnection
            excepted error message: Unhandled exception: java.sql.SQLException
             */
            try {
                if (sqlConnection != null) {
                    sqlConnection.close();
                }
                if (mongoClient != null) {
                    mongoClient.close();
                }
            } catch (Exception ex) {
                System.out.println("Got Exception. ");
                ex.printStackTrace();
            }
        }
    }
}