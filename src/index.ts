import dotenv from 'dotenv'
dotenv.config()


import { Kafka } from 'kafkajs';
import { v4 as uuidv4 } from 'uuid';
import { createClient } from '@supabase/supabase-js'
import { Database } from './types/database.js'
import { useViemService } from './services/viem_service.js'
import { Hex, parseTransaction } from 'viem'
import consola  from 'consola'

const supabaseUrl = process.env.SUPABASE_URL
const supabaseKey = process.env.SUPABASE_KEY

const logger = consola.create({
    defaults: {
      tag: "camunda_market_service",
    },
  });

if (!supabaseUrl || !supabaseKey) {
    throw new Error('Supabase URL and Key must be provided in .env file')
}

  
// Kafka configuration
const kafka = new Kafka({
clientId: process.env.KAFKA_CLIENT_ID || "estate-key-storage",
brokers: [process.env.KAFKA_BROKER || "localhost:9092"]
});

const TOPICS = {
    BUY_OFFER_DATA_VALIDATED: "buy-offer-data-validated",
    BUY_OFFER_WALLET_FETCHED: "buy-offer-wallet-fetched",
    BUY_OFFER_TRANSACTION_PREPARED: "buy-offer-transaction-prepared",
    BUY_OFFER_TRANSACTION_SIGNED: "buy-offer-transaction-signed",
    BUY_OFFER_TRANSACTION_CONFIRMED: "buy-offer-transaction-confirmed",
    BUY_OFFER_TRANSACTION_LOGGED: "buy-offer-transaction-logged",
    BUY_OFFER_ERROR: "buy-offer-error",
}

export const produceMessage = async <T extends Record<string, any>>(topic: string, message: T & { processId?: string }) => {
    const producer = kafka.producer();
    
    try {
      await producer.connect();
      await producer.send({
        topic,
        messages: [{ 
          key: message.processId || uuidv4(), 
          value: JSON.stringify(message) 
        }]
      });
      logger.info(`Message sent to topic ${topic}`);
    } finally {
      await producer.disconnect();
    }
};

export const createDirectServiceClient = () => {
    
    if (!supabaseUrl || !supabaseKey) {
      throw new Error('Missing Supabase environment variables')
    }
    return createClient<Database>(supabaseUrl, supabaseKey)
}

const runGetUserWalletAddressConsumer = async () => {
  const consumer = kafka.consumer({ groupId: 'fetch-wallet-address-group' });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.BUY_OFFER_DATA_VALIDATED, fromBeginning: true });
  await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
      try {
          if (!message.value) return;
          const payload = JSON.parse(message.value.toString());
          logger.info("GetUserWalletAddress event received:", payload);
          
          const { userId, offerId, price, processId } = payload;
          logger.info(`Fetching wallet address for userId: ${userId}`);

          const supabaseClient = createDirectServiceClient();
          // Check if wallet already exists
          const { data: wallet, error: walletError } = await supabaseClient
          .from("wallets")
          .select("*")
          .eq("user_id", userId)
          .single();
          
          if (walletError || !wallet) {
          throw new Error(`Wallet not found for user ${userId}: ${walletError?.message || "Not found"}`);
          }
          
          const { getAccount } = useViemService();
          const privateKey = wallet.private_key as Hex;
          const account = getAccount(privateKey);
          const walletAddress = account.address;

          // Send result to next topic in workflow
          await produceMessage(TOPICS.BUY_OFFER_WALLET_FETCHED, {
          walletAddress,
          userId,
          offerId,
          price,
          processId
          });
          
          logger.success("Wallet address fetched successfully:", walletAddress);
      } catch (error) {
          logger.error("Error in fetching wallet address:", error);
          // Send error event
          if (message.value) {
          const payload = JSON.parse(message.value.toString());
          await produceMessage(TOPICS.BUY_OFFER_ERROR, {
              error: error instanceof Error ? error.message : 'Unknown error',
              stage: 'get_wallet',
              payload,
              processId: payload.processId
          });
          }
      }
      },
  });
};

const runSignTransactionConsumer = async () => {
  const consumer = kafka.consumer({ groupId: 'transaction-signing-group' });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.BUY_OFFER_TRANSACTION_PREPARED, fromBeginning: true });
  
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        if (!message.value) return;
        const payload = JSON.parse(message.value.toString());
        logger.info("SignTransaction event received:", payload);
        
        const { serializedTransaction, from, offerId, processId } = payload;
        const userId = payload.userId; // Make sure this is passed through the entire flow
        
        logger.info(`Signing transaction for address: ${from}`);
        
        // Parse the transaction
        const transaction = parseTransaction(serializedTransaction);
        logger.info("Parsed Transaction data:", transaction);
        
        // Get the user's wallet from Supabase
        const supabaseClient = createDirectServiceClient();
        const { data: wallet, error: walletError } = await supabaseClient
          .from("wallets")
          .select("*")
          .eq("user_id", userId)
          .single();
          
        if (walletError || !wallet) {
          throw new Error(`Wallet not found for user ${userId}: ${walletError?.message || "Not found"}`);
        }
        
        // Sign the transaction
        const { getAccount } = useViemService();
        const privateKey = wallet.private_key as Hex;
        const account = getAccount(privateKey);
        const signedTransaction = await account.signTransaction(transaction);
        
        logger.info("Transaction signed successfully");
        
        // Send signed transaction to next topic
        await produceMessage(TOPICS.BUY_OFFER_TRANSACTION_SIGNED, {
          signedTransaction,
          from,
          offerId,
          userId,
          processId
        });
        
        logger.success("Transaction signing completed");
      } catch (error) {
        logger.error("Error in signing transaction:", error);
        // Send error event
        if (message.value) {
          const payload = JSON.parse(message.value.toString());
          await produceMessage(TOPICS.BUY_OFFER_ERROR, {
            error: error instanceof Error ? error.message : 'Unknown error',
            stage: 'sign_transaction',
            payload,
            processId: payload.processId
          });
        }
      }
    },
  });
};


const main = async () => {
  try {
    await runGetUserWalletAddressConsumer();
    await runSignTransactionConsumer();
    
    logger.success("All Kafka wallet service consumers started successfully");
  } catch (error) {
    logger.error("Error starting Kafka consumers:", error);
    process.exit(1);
  }
};

// Start the application
main();