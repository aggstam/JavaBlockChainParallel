# JavaBlockChainParallel
This console application simulates a Product Database using Block Chain.
<br>
Users can interact by selecting an action with their input.
<br>
Each action is performed using parallelism.
<br>
Note: project requires *java* to be installed.
<br>
External jar gson-2.8.2 is provided

# Usage
First we compile the project:
```
% javac -cp libs/gson-2.8.2.jar src/**/*.java -d bin
```
Then we can execute:
```
% java -cp libs/gson-2.8.2.jar:.:bin com.blockchain.Main
```

# Execution example
```
❯ java -cp libs/gson-2.8.2.jar:.:bin com.blockchain.Main
Jan 22, 2023 4:43:34 PM com.blockchain.Main main
INFO: Product Block Chain application started.
Jan 22, 2023 4:43:34 PM com.blockchain.Main retrieveInputAction
INFO: Enter action:
statistics
Jan 22, 2023 4:43:43 PM com.blockchain.Main retrieveInputAction
INFO: User input: statistics
Jan 22, 2023 4:43:43 PM com.blockchain.Main retrieveProductBlockStatistics
INFO: Search for a Product Block to retrieve its Statistics.
Jan 22, 2023 4:43:43 PM com.blockchain.Main retrieveProductBlockStatistics
INFO: Provide Product Code to search:
001
Jan 22, 2023 4:44:00 PM com.blockchain.StopWatch start
INFO: Process displayProductStatistics started.
Jan 22, 2023 4:44:00 PM com.blockchain.ProductBlockChain displayProductStatistics
INFO: Product found:
{
  "hash": "00000022a1fdf528b5571951e846f64abd31cb02e91dab82db1cbccb56e40d00",
  "previousHash": "00000096714c61b4057b79eda9df5aa0bf1db428daadaf9fdd8aa59f166073ed",
  "blockId": 157,
  "productCode": "001",
  "productTitle": "First Product",
  "productPrice": 10.5,
  "productCategory": "001",
  "productDescription": "Testing Blockchain actions",
  "productPreviousRecordId": 154,
  "timestamp": 1575843735065,
  "nonce": 42181197
}
Jan 22, 2023 4:44:00 PM com.blockchain.ProductBlockChain displayProductStatistics
INFO: - Price changes:
Jan 22, 2023 4:44:00 PM com.blockchain.ProductBlockChain displayProductStatistics
INFO: 	2019/12/09 00:22:15 -> 10.5
Jan 22, 2023 4:44:00 PM com.blockchain.ProductBlockChain displayProductStatistics
INFO: 	2019/12/09 00:21:54 -> 10.5
Jan 22, 2023 4:44:00 PM com.blockchain.ProductBlockChain displayProductStatistics
INFO: 	2019/12/09 00:20:09 -> 10.5
Jan 22, 2023 4:44:00 PM com.blockchain.ProductBlockChain displayProductStatistics
INFO: 	2019/12/09 00:18:46 -> 10.5
Jan 22, 2023 4:44:00 PM com.blockchain.ProductBlockChain displayProductStatistics
INFO: Statistics:
Jan 22, 2023 4:44:00 PM com.blockchain.ProductBlockChain displayProductStatistics
INFO: - Price median: 10.5
Jan 22, 2023 4:44:00 PM com.blockchain.ProductBlockChain displayProductStatistics
INFO: - Max price: 10.5
Jan 22, 2023 4:44:00 PM com.blockchain.ProductBlockChain displayProductStatistics
INFO: - Min price: 10.5
Jan 22, 2023 4:44:00 PM com.blockchain.StopWatch stop
INFO: Process displayProductStatistics finished. Total time elapsed: 0.06678487 seconds
Jan 22, 2023 4:44:00 PM com.blockchain.Main retrieveInputAction
INFO: Enter action:
quit
Jan 22, 2023 4:44:04 PM com.blockchain.Main retrieveInputAction
INFO: User input: quit
Jan 22, 2023 4:44:04 PM com.blockchain.Main main
INFO: Product Block Chain application terminated.
```
