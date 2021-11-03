// -------------------------------------------------------------
//
// This is the main Block Structure used by the application.
// Product data: Code, Title, Price, Category and Description.
//
// Author: Aggelos Stamatiou, November 2019
//
// --------------------------------------------------------------

package com.blockchain;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class ProductBlock {

    private static Logger logger = Logger.getLogger(ProductBlock.class.getName());

    private String hash;
    private String previousHash;
    private Integer blockId;
    private String productCode;
    private String productTitle;
    private Double productPrice;
    private String productCategory;
    private String productDescription;
    private Integer productPreviousRecordId;
    private long timestamp;
    private int nonce;

    public ProductBlock(String previousHash, Integer blockId, String productCode, String productTitle,
                        Double productPrice, String productCategory, String productDescription, Integer productPreviousRecordId) throws Exception {
        this.previousHash = previousHash;
        this.blockId = blockId;
        this.productCode = productCode;
        this.productTitle = productTitle;
        this.productPrice = productPrice;
        this.productCategory = productCategory;
        this.productDescription = productDescription;
        this.productPreviousRecordId = productPreviousRecordId;
        this.timestamp = new Date().getTime();
        this.hash = calculateBlockHash(null);
    }

    public String getHash() {
        return this.hash;
    }

    public String getPreviousHash() {
        return this.previousHash;
    }

    public Integer getBlockId() {
        return this.blockId;
    }

    public String getProductCode() {
        return this.productCode;
    }

    public String getProductTitle() {
        return this.productTitle;
    }

    public String getProductCategory() {
        return this.productCategory;
    }

    public String getProductDescription() {
        return this.productDescription;
    }

    public Double getProductPrice() {
        return productPrice;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Integer getProductPreviousRecordId() { return productPreviousRecordId; }

    // This method tries to find(mine) the appropriate hash for a given prefix.
    // Threads are used. Each one tries to find the hash. The first to find it
    // saves it in Block, which terminates the others.
    public String mineBlockParallel(int prefix, int threadCount) throws Exception {

        String prefixString = new String(new char[prefix]).replace('\0', '0');
        // This is the code each Miner will run.
        Runnable minerRunnable = new Runnable() {
            @Override
            public void run() {
                try {
                    String threadName = Thread.currentThread().getName().replaceAll("pool-[0-9]+-thread-", "");
                    logger.info("Miner " + threadName + " started.");
                    // Each Miner creates a local hash and nonce.
                    int threadId = Integer.parseInt(threadName);
                    // First thread will try serial while the other will try with random numbers
                    int localNonce = (threadId == 1 ) ? 0 : new Random().nextInt() * threadId; // Executors Pool starts naming from 1, not 0;
                    String localHash = calculateBlockHash(localNonce);
                    // Each Miner check if its local hash or the Block hash is valid.
                    while (!(localHash.substring(0, prefix).equals(prefixString) || hash.substring(0, prefix).equals(prefixString))) {
                        localNonce += (threadId == 1 ) ? 1 : new Random().nextInt() * threadId;
                        localHash = calculateBlockHash(localNonce);
                    }
                    logger.info("Miner " + threadName + " finished. Hash->" + localHash + ", nonce->" + Integer.toString(localNonce));
                    // If a Miner creates a local hash that is valid, it pass it to the Block hash.
                    synchronized (this) {
                        // Only the first Miner that found the hash can update the Block hash.
                        if (!hash.substring(0, prefix).equals(prefixString)) {
                            hash = localHash;
                            nonce = localNonce;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.info("There was an exception on Block " + blockId + " hash calculation.");
                }
            }
        };

        // Create a Miners Pool and execute the Runnable.
        ExecutorService minersPool = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            minersPool.execute(minerRunnable);
        }
        minersPool.shutdown();
        while (!minersPool.isTerminated()) {
            // Waiting Miners Pool termination.
        }

        logger.info("Mined hash->" + hash + ", validity->" + hash.equals(calculateBlockHash(null)));

        return hash;
    }

    // Calculating Block hash by its current state.
    public String calculateBlockHash(Integer nonce) throws Exception {
        String nonceToHash = (nonce != null) ? Integer.toString(nonce) : Integer.toString(this.nonce);
        String dataToHash = previousHash + timestamp + nonceToHash + blockId + productCode + productTitle + productPrice + productCategory + productDescription + productPreviousRecordId;
        MessageDigest digest = null;
        byte[] bytes = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
            bytes = digest.digest(dataToHash.getBytes("UTF-8"));
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new Exception("There was an exception on Block hash calculation.");
        }
        StringBuffer buffer = new StringBuffer();
        for (byte b : bytes) {
            buffer.append(String.format("%02x", b));
        }
        return buffer.toString();
    }
}
