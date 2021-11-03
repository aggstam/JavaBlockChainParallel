// -------------------------------------------------------------
//
// This is the main Block Chain Structure used by the application.
// A .json file is used for saving the Block Chain.
// On application startup the Block Chain is retrieved from the file.
// On a new Block creation, the Block chain is saved to the file.
// Available actions: View, Add, Search, Show Statistics and Validate.
// Each action is performed using parallelism.
//
// Author: Aggelos Stamatiou, November 2019
//
// --------------------------------------------------------------

package com.blockchain;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.io.*;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProductBlockChain {

    private static Logger logger = Logger.getLogger(ProductBlockChain.class.getName());
    private static Gson jsonPrettyPrinter = new GsonBuilder().setPrettyPrinting().create();
    private static final String DATABASE_FILE = "BlockChainDB.json"; // Program uses a .json file that acts as/simulates Database.
    private static final int prefix = 6; // This is the prefix zeros count a hash must have in order to be considered valid.
    private static List<ProductBlock> blockChain;

    // Retrieving Block Chain current state from a File that acts as/simulates a Database.
    public ProductBlockChain() {
        try {
            Reader reader = new FileReader(DATABASE_FILE);
            Type listType = new TypeToken<ArrayList<ProductBlock>>(){}.getType();
            blockChain = new Gson().fromJson(reader, listType);
            reader.close();
        } catch (IOException e) {
            // If the file is not found or is corrupted, a new Block Chain will be created.
            logger.info("No Database file found, creating new Block Chain.");
            blockChain = new ArrayList<>();
        }
    }

    // Creates multiple new Product Blocks to the Block Chain.
    // Each Product Block is mined and saved to the Block Chain.
    public void addMultipleProducts(List<Map<String, String>> productInformationList, int threadCount) throws Exception {
        for (Map<String, String> productInformation : productInformationList) {
            addProduct(productInformation, threadCount);
        }
    }

    // Creates a new Product Block to the Block Chain.
    // The Product Block is mined and saved to the Block Chain.
    public void addProduct(Map<String, String> productInformation, int threadCount) throws Exception {
        if (blockChain != null) {
            ProductBlock productBlock;
            String productCode = productInformation.get("productCode");
            String productTitle = productInformation.get("productTitle");
            Double productPrice = (!productInformation.get("productPrice").equals("")) ? Double.parseDouble(productInformation.get("productPrice")) : 0 ;
            String productCategory = productInformation.get("productCategory");
            String productDescription = productInformation.get("productDescription");
            if (!blockChain.isEmpty()) {
                // Retrieving latest Product Person for that productCode, if it exists.
                ProductBlock productLatestRecord = blockChain.parallelStream()
                        .filter(block -> productCode.equals(block.getProductCode()))
                        .max(Comparator.comparing(ProductBlock::getBlockId))
                        .orElse(null);
                // If the record exists, retrieve its Block Id to pass it to the next Block.
                Integer productPreviousRecordId = (productLatestRecord != null) ? productLatestRecord.getBlockId() : null;
                productBlock = new ProductBlock(blockChain.get(blockChain.size()-1).getHash(), blockChain.size(), productCode, productTitle, productPrice, productCategory, productDescription, productPreviousRecordId);
            } else {
                // Genesis Block.
                productBlock = new ProductBlock(null, blockChain.size(), productCode, productTitle, productPrice, productCategory, productDescription, null);
            }
            // Calculating Block hash.
            productBlock.mineBlockParallel(prefix, threadCount);
            blockChain.add(productBlock);
            logger.info("Product Block has been successfully created. Saving it to Database.");
            // Saving new Block to *Database*.
            saveChainToJsonFile();
        } else {
            throw new Exception("Block Chain not initialized correctly.");
        }
    }

    // Saving Block Chain current state to a File that acts as/simulates a Database.
    private static void saveChainToJsonFile() throws Exception {
        try {
            Writer writer = new FileWriter(DATABASE_FILE);
            String blockChainJson = jsonPrettyPrinter.toJson(blockChain);
            writer.append(blockChainJson);
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new Exception("Block Chain could not be saved to Database.");
        }
    }

    // Loop through Block Chain to check hashes validity.
    public Boolean isChainValid() throws Exception {
        String hashTarget = new String(new char[prefix]).replace('\0', '0');
        AtomicReference<Boolean> isBlockChainValid = new AtomicReference<>(true);
        blockChain.parallelStream().forEach( block -> {
            // Compare registered hash and calculated hash.
            try {
                if (!block.getHash().equals(block.calculateBlockHash(null))) {
                    logger.info("Block " + block.getBlockId() + " current hashes not equal.");
                    if (isBlockChainValid.get()) isBlockChainValid.set(false);
                }
            } catch (Exception e) {
                e.printStackTrace();
                if (isBlockChainValid.get()) isBlockChainValid.set(false);
            }
            // Compare previous hash and registered previous hash.
            if (blockChain.indexOf(block) > 0) {
                if (!blockChain.get(blockChain.indexOf(block) - 1).getHash().equals(block.getPreviousHash())) {
                    logger.info("Block " + block.getBlockId() + " previous hashes not equal.");
                    if (isBlockChainValid.get()) isBlockChainValid.set(false);
                }
            }
            // Check if hash is solved.
            if (!block.getHash().substring( 0, prefix).equals(hashTarget)) {
                logger.info("Block " + block.getBlockId() + " hasn't been mined.");
                if (isBlockChainValid.get()) isBlockChainValid.set(false);
            }
        });

        return isBlockChainValid.get();
    }

    // Search a Product Block by "Product Code", "Product Title" and "Product Category".
    public void searchProduct(String productCode, String productTitle, String productCategory, String productDescription, Boolean retrieveLatest) {
        if (blockChain != null && !blockChain.isEmpty()) {
            // Creating a stream for the requested filters.
            Stream<ProductBlock> productRecordsStream = blockChain.parallelStream()
                            .filter(block ->block.getProductCode().contains(productCode)
                            && block.getProductTitle().contains(productTitle)
                            && block.getProductCategory().contains(productCategory)
                            && block.getProductDescription().contains(productDescription));
            // Collecting only the latest or the first record for each Product, based on findFirst variable.
            Map<String, ProductBlock> productRecordsMaps = (retrieveLatest) ?
                    productRecordsStream.collect(Collectors.toMap(ProductBlock::getProductCode, Function.identity(), (ProductBlock b1, ProductBlock b2) -> b1.getBlockId() < b2.getBlockId() ? b1 : b2))
                    : productRecordsStream.collect(Collectors.toMap(ProductBlock::getProductCode, Function.identity(), (ProductBlock b1, ProductBlock b2) -> b1.getBlockId() > b2.getBlockId() ? b1 : b2));
            // Printing Search Results.
            List<ProductBlock> productRecordList = new ArrayList<>(productRecordsMaps.values());
            if (!productRecordList.isEmpty()) {
                logger.info("Product records found: \n" + jsonPrettyPrinter.toJson(productRecordList));
            } else {
                logger.info("No Products matching your search found.");
            }

        } else {
            logger.info("No products exists in DataBase.");
        }
    }

    // Search a Product Block by "Product Code" and retrieve some statistics for it.
    public void displayProductStatistics(String productCode) {
        if (blockChain != null && !blockChain.isEmpty()) {
            // Retrieving latest Product Person for that productCode, if it exists.
            ProductBlock productLatestRecord = blockChain.parallelStream()
                    .filter(block -> productCode.equals(block.getProductCode()))
                    .max(Comparator.comparing(ProductBlock::getBlockId))
                    .orElse(null);
            if (productLatestRecord != null) {
                // Product Latest Record.
                logger.info("Product found: \n" + jsonPrettyPrinter.toJson(productLatestRecord));
                // Product Price Changes.
                logger.info("- Price changes: ");
                // Product Price sum for calculating the Median.
                Double recordsPriceSum = 0.0;
                // Product records count;
                Integer recordsCount = 0;
                ProductBlock currentBlock = productLatestRecord;
                // Product Max Price.
                Double recordsMaxPrice = currentBlock.getProductPrice();
                // Product Lowest Price.
                Double recordsMinPrice = currentBlock.getProductPrice();
                SimpleDateFormat formatter = new SimpleDateFormat("YYYY/MM/dd HH:mm:ss");
                // Back tracking to find older records.
                do {
                    recordsPriceSum += currentBlock.getProductPrice();
                    if (currentBlock.getProductPrice() > recordsMaxPrice) recordsMaxPrice = currentBlock.getProductPrice();
                    if (currentBlock.getProductPrice() < recordsMinPrice) recordsMinPrice = currentBlock.getProductPrice();
                    logger.info("\t" + formatter.format(currentBlock.getTimestamp()) + " -> " + currentBlock.getProductPrice());
                    recordsCount ++;
                    currentBlock = blockChain.get(currentBlock.getProductPreviousRecordId());
                } while (currentBlock.getProductPreviousRecordId() != null);
                logger.info("Statistics: ");
                logger.info("- Price median: " + recordsPriceSum / recordsCount);
                logger.info("- Max price: " + recordsMaxPrice);
                logger.info("- Min price: " + recordsMinPrice);
            } else {
                logger.info("Product not found.");
            }
        } else {
            logger.info("No products exists in DataBase.");
        }
    }

    @Override
    public String toString() {
        return jsonPrettyPrinter.toJson(blockChain);
    }
}
