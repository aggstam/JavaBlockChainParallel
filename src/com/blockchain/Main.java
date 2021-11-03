// -------------------------------------------------------------
//
// This console application simulates a Product Database using Block Chain.
// Users can interact by selecting an action with their input.
// Each action is performed using parallelism.
//
// Author: Aggelos Stamatiou, November 2019
//
// --------------------------------------------------------------

package com.blockchain;

import java.util.*;
import java.util.logging.Logger;

public class Main {

    private static Logger logger = Logger.getLogger(Main.class.getName());
    private static StopWatch stopwatch = new StopWatch();
    private static Scanner inputScanner = new Scanner(System.in);
    // Define how many threads to use in parallel actions.
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors(); // System threads.
    //private static final int THREAD_COUNT = 4; // Exact thread count to use.
    private static ProductBlockChain blockChain = new ProductBlockChain();

    // This is the UI thread interacting with the user.
    public static void main(String[] args) {
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", Integer.toString(THREAD_COUNT)); // Set thread count used in Parallel Streams.
        logger.info("Product Block Chain Parallel (" + THREAD_COUNT + " Threads) application started.");
        try {
            // Retrieving user input to define the action. Application terminates if user enters "quit".
            String action = retrieveInputAction();
            while (!action.equals("quit")) {
                if (action.equals("view")) {
                    printCurrentBlockChain(); // View Block Chain current state.
                } else if (action.equals("add")) {
                    addNewProductBlock(); // Add a new Product Block to the Block Chain.
                } else if (action.equals("addMultiple")) {
                    addMultipleNewProductBlocks(); // Add multiple Product Blocks to the Block Chain.
                } else if (action.equals("search")) {
                    searchProductBlock(); // Search a Product Block by criteria.
                } else if (action.equals("statistics")) {
                    retrieveProductBlockStatistics(); // Search a Product Block by "Product Code" and retrieve some statistics for it.
                } else if (action.equals("validate")) {
                    checkCurrentBlockChainValidity(); // Validates Block Chain current state.
                }
                action = retrieveInputAction();
            }

        } catch (Exception e) {
            // When an exception occurs, its stacktrace is printed and the application terminates.
            e.printStackTrace();
            logger.info("There was an exception (" + e.getMessage() +"). Application terminating.");
        }
        logger.info("Product Block Chain application terminated.");
    }

    // Retrieving users input in order to define the action.
    private static String retrieveInputAction () {
        logger.info("Enter action: ");
        String input = inputScanner.nextLine();
        logger.info("User input: " + input);
        if (input.equalsIgnoreCase("view")) {
            return "view"; // View Block Chain current state.
        } else if (input.equalsIgnoreCase("add")) {
            return "add"; // Add a new Product Block to the Block Chain.
        } else if (input.equalsIgnoreCase("addMultiple")) {
            return "addMultiple"; // Add multiple Product Blocks to the Block Chain.
        } else if (input.equalsIgnoreCase("search")) {
            return "search"; // Search a Product Block by criteria.
        } else if (input.equalsIgnoreCase("statistics")) {
            return "statistics"; // Search a Product Block by "Product Code" and retrieve some statistics for it.
        } else if (input.equalsIgnoreCase("validate")) {
            return "validate"; // Validates Block Chain current state.
        } else if (input.equalsIgnoreCase("quit")) {
            return "quit"; // Quits the application.
        } else {
            // When the input is not recognised, a legend with available actions is printed.
            logger.info("Unrecognised action. Accepted actions:\n" +
                    "view -> Current Product Block Chain is printed.\n" +
                    "add -> Create a new Product Block.\n" +
                    "addMultiple -> Create multiple Product Blocks.\n" +
                    "search -> Search for a Product Block.\n" +
                    "statistics -> Show statistics of a Product Block.\n" +
                    "validate -> Check current Block Chain validity.\n" +
                    "quit -> Terminates the application.");
        }
        return "";
    }

    // Add multiple Product Blocks to the Block Chain.
    // User select how many Product Blocks to create and inputs Information for each one.
    // When User finishes inputting information, all Product Blocks are created.
    private static void addMultipleNewProductBlocks() throws Exception {
        logger.info("Provide Products Count(positive number without decimals, all other characters will be filtered): ");
        String productCount = inputScanner.nextLine().replaceAll("[^\\d]", "");
        int count = (!productCount.equals("")) ? Integer.parseInt(productCount) : 0 ;
        while (count == 0) {
            logger.info("Count must be a positive number without decimals, all other characters will be filtered. Please retry: ");
            productCount = inputScanner.nextLine().replaceAll("[^\\d]", "");
            count = (!productCount.equals("")) ? Integer.parseInt(productCount) : 0 ;
        }
        List<Map<String, String>> productInformationList = new ArrayList<>();
        for (int i = 0 ; i < count ; i++) {
            logger.info("Creating Product: " + (i + 1));
            Map<String, String> productInformation = retrieveBlockInformation();
            productInformationList.add(productInformation);
        }
        logger.info("Please wait...");
        stopwatch.start("addMultipleProducts");
        blockChain.addMultipleProducts(productInformationList, THREAD_COUNT);
        stopwatch.stop("addMultipleProducts");
    }

    // Add a new Product Block to the Block Chain.
    private static void addNewProductBlock() throws Exception {
        logger.info("Creating new Product Block.");
        Map<String, String> productInformation = retrieveBlockInformation();
        logger.info("Please wait...");
        stopwatch.start("addProduct");
        blockChain.addProduct(productInformation, THREAD_COUNT);
        stopwatch.stop("addProduct");
    }

    // User inputs the appropriate fields for the Product Block creation.
    private static  Map<String, String> retrieveBlockInformation() {
        Map<String, String> productInformation = new HashMap<>();
        // Retrieve Product fields from User.
        logger.info("Provide Product Code: ");
        productInformation.put("productCode", inputScanner.nextLine());
        logger.info("Provide Product Title: ");
        productInformation.put("productTitle", inputScanner.nextLine());
        logger.info("Provide Product Category: ");
        productInformation.put("productCategory", inputScanner.nextLine());
        logger.info("Provide Product Description: ");
        productInformation.put("productDescription", inputScanner.nextLine());
        logger.info("Provide Product Price (positive number with decimals, all other characters will be filtered): ");
        // Price value is filtered. Only numerals and the last dot are kept.
        String productPrice = inputScanner.nextLine().replaceAll(",", ".").replaceAll("[^\\d.]", "");
        if (productPrice.contains(".")) {
            productPrice = productPrice.substring(0, productPrice.lastIndexOf('.')).replaceAll("\\.", "") + "." + productPrice.substring(productPrice.lastIndexOf('.') + 1);
        }
        productInformation.put("productPrice", productPrice);
        logger.info("Creating Product: Code-> " + productInformation.get("productCode")
                + ", Title-> " + productInformation.get("productTitle") + ", Price-> " + productInformation.get("productPrice")
                + ", Category-> " + productInformation.get("productCategory") + ", Description-> " + productInformation.get("productDescription") + ".");

        return productInformation;
    }

    // Search a Product Block by "Product Code", "Product Title", "Product Category" and "Product Description".
    // User can leave a criteria empty and select if the latest or the first record should be retrieved.
    private static void searchProductBlock() {
        logger.info("Search for a Product Block.");
        // Retrieve Product fields from User.
        logger.info("Provide Product Code to search(leave empty if not required): ");
        String productCode = inputScanner.nextLine();
        logger.info("Provide Product Title to search(leave empty if not required): ");
        String productTitle = inputScanner.nextLine();
        logger.info("Provide Product Category to search(leave empty if not required): ");
        String productCategory = inputScanner.nextLine();
        logger.info("Provide Product Description to search(leave empty if not required): ");
        String productDescription = inputScanner.nextLine();
        logger.info("Retrieve Latest product (select \"false\" if you want the oldest Record, otherwise leave empty): ");
        String retrieveLatest = inputScanner.nextLine();
        while (!(retrieveLatest.equals("") || retrieveLatest.equalsIgnoreCase("false"))) {
            logger.info("You must select \"false\" if you want the oldest Record, otherwise leave empty. Please retry: ");
            retrieveLatest = inputScanner.nextLine();
        }
        stopwatch.start("searchProduct");
        blockChain.searchProduct(productCode, productTitle, productCategory, productDescription, retrieveLatest.equals(""));
        stopwatch.stop("searchProduct");
    }

    // Search a Product Block by "Product Code" and retrieve some statistics for it.
    private static void retrieveProductBlockStatistics() {
        logger.info("Search for a Product Block to retrieve its Statistics.");
        // Retrieve Product code from User.
        logger.info("Provide Product Code to search: ");
        String productCode = inputScanner.nextLine();
        stopwatch.start("displayProductStatistics");
        blockChain.displayProductStatistics(productCode);
        stopwatch.stop("displayProductStatistics");
    }

    // Validates Block Chain current state.
    private static void checkCurrentBlockChainValidity() throws Exception {
        logger.info("Checking current Block Chain validity.");
        stopwatch.start("isChainValid");
        Boolean isChainValid = blockChain.isChainValid();
        stopwatch.stop("isChainValid");
        if (isChainValid) {
            logger.info("BlockChain has been successfully validated.");
        } else {
            logger.info("BlockChain is not valid.");
        }
    }

    // View Block Chain current state.
    private static void printCurrentBlockChain() {
        logger.info("Current Product BlockChain: ");
        logger.info(blockChain.toString());
    }
}
