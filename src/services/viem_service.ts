import type { Hex } from 'viem'
import { createWalletClient, http, publicActions } from 'viem'
import {
    generatePrivateKey,
    privateKeyToAddress,
    privateKeyToAccount,
} from 'viem/accounts'
import { sepolia } from 'viem/chains'

export function useViemService() {
    const client = createWalletClient({
        chain: sepolia,
        transport: http(),
    }).extend(publicActions)

    const generateAccount = () => {
        return generatePrivateKey()
    }

    const getAddress = (privateKey: Hex) => {
        return privateKeyToAddress(privateKey).toLowerCase() as Hex
    }

    const getAccount = (privateKey: Hex) => {
        return privateKeyToAccount(privateKey)
    }

    return { client, generateAccount, getAddress, getAccount }
}
