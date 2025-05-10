import dotenv from 'dotenv'
dotenv.config()

import { createClient } from '@supabase/supabase-js'
import { Database } from './types/database.js'
import { useViemService } from './services/viem_service.js'
import { Hex, parseTransaction } from 'viem'
import { Client, Variables, TaskService, Task  } from "camunda-external-task-client-js";
import consola  from 'consola'
import { error } from 'console'

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
const client = new Client({
    baseUrl: process.env.CAMUNDA_API_URL || 'http://localhost:8080/engine-rest',
    asyncResponseTimeout: 10000,
    maxTasks: 10
})
  

export const createDirectServiceClient = () => {
    
    if (!supabaseUrl || !supabaseKey) {
      throw new Error('Missing Supabase environment variables')
    }
    return createClient<Database>(supabaseUrl, supabaseKey)
}

client.subscribe("getUserWalletAddress", async function({ task, taskService }: { task: Task, taskService: TaskService }) {
    try {
      logger.info("getUserWalletAddress task executing...");
      
      const userId: string = task.variables.get("userId");
      
      logger.info(`Fetching wallet address for userId: ${userId}`);

      const supabaseClient = createDirectServiceClient();
      // Check if wallet already exists
      const { data: wallet } = await supabaseClient
        .from("wallets")
        .select("*")
        .eq("user_id", userId)
        .single();
      if (!wallet) {
        throw error({
          statusCode: 404,
          statusMessage: "Wallet not found",
        });
      }
      const { getAccount } = useViemService();
      const privateKey = wallet.private_key as Hex;
      const account = getAccount(privateKey);
      const userAddress = account.address;



      const processVariables = new Variables();
      processVariables.set("walletAddress", userAddress);
      await taskService.complete(task, processVariables);
      logger.success("Wallet address fetched successfully:", userAddress);
    } catch (error) {
      logger.error("Error in getUserWalletAddress task:", error);
      if (error instanceof Error) {
        await taskService.handleBpmnError(
          task, 
          "FETCH WALLET ERROR",
          `Wallet address fetching failed: ${error.message}`
        );
      }
    }
  });

  client.subscribe("signTransaction", async function({ task, taskService }: { task: Task, taskService: TaskService }) {
    try {
      logger.info("signTransaction task executing...");


      const from = task.variables.get("transactionData").from;
      const userId: string = task.variables.get("userId");
      const serializedTransaction = task.variables.get("transactionData").serializedTransaction;
      logger.info(`Signing transaction for address: ${from}`);
      logger.info("Serialized Transaction data:", serializedTransaction);
      const transaction = parseTransaction(serializedTransaction);
      logger.info("Parsed Transaction data:", transaction);
      const supabaseClient = createDirectServiceClient();

      const { data: wallet } = await supabaseClient
      .from("wallets")
      .select("*")
      .eq("user_id", userId)
      .single();
    if (!wallet) {
      throw error({
        statusCode: 404,
        statusMessage: "Wallet not found",
      });
    }
    const { getAccount } = useViemService();
    const privateKey = wallet.private_key as Hex;
    const account = getAccount(privateKey);
    const signedTransaction = await account.signTransaction(transaction);
    logger.info("Signed Transaction data:", signedTransaction);
    const processVariables = new Variables();
    processVariables.set("signedTransaction", signedTransaction);
    await taskService.complete(task, processVariables);
    logger.success("Transaction signing completed successfully:", signedTransaction);
  
    } catch (error) {
      logger.error("Error in signTransaction task:", error);
      if (error instanceof Error) {
        await taskService.handleBpmnError(
          task, 
          "SIGN TRANSACTION ERROR",
          `Transaction signing failed: ${error.message}`
        );
      }
    }
     
  });
