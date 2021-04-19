package upgrad;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import java.util.Arrays;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Aggregates.*;
public class CRUDHelper {

    /**
     * Display ALl products
     * @param collection
     */
    public static void displayAllProducts(MongoCollection<Document> collection) {
        System.out.println("------ Displaying All Products ------");
        // Call printSingleCommonAttributes to display the attributes on the Screen
        for (Document cur : collection.find()) {
           PrintHelper.printSingleCommonAttributes(cur);
        }

    }

    /**
     * Display top 5 Mobiles
     * @param collection
     */
    public static void displayTop5Mobiles(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Top 5 Mobiles ------");
        // Call printAllAttributes to display the attributes on the Screen
        for (Document cur : collection.find(eq("Category", "Mobile")).limit(5)) {
            PrintHelper.printAllAttributes(cur);
        }
    }

    /**
     * Display products ordered by their categories in Descending order without auto generated Id
     * @param collection
     */
    public static void displayCategoryOrderedProductsDescending(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Products ordered by categories ------");
        // Call printAllAttributes to display the attributes on the Screen
        for (Document cur : collection.find().projection(excludeId()).sort(new Document("Category", -1))) {
            PrintHelper.printAllAttributes(cur);
        }
    }


    /**
     * Display number of products in each group
     * @param collection
     */
    public static void displayProductCountByCategory(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Product Count by categories ------");
        // Call printProductCountInCategory to display the attributes on the Screen
        for (Document cur : collection.aggregate(Arrays.asList(Aggregates.group("$Category",
                Accumulators.sum("Count", 1L)), sort(Sorts.ascending("Category"))))) {
            PrintHelper.printProductCountInCategory(cur);
        }
    }

    /**
     * Display Wired Headphones
     * @param collection
     */
    public static void displayWiredHeadphones(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Wired headphones ------");
        // Call printAllAttributes to display the attributes on the Screen

        for (Document cur : collection.find(eq("ConnectorType", "Wired"))) {
            PrintHelper.printAllAttributes(cur);
        }
    }
}