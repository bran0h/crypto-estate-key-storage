import dotenv from 'dotenv'
dotenv.config()

import express from 'express'
import { createClient } from '@supabase/supabase-js'
import { Database } from './types/database'
import cookieParser from 'cookie-parser'
import { useViemService } from './services/viem_service'
import { EncodeFunctionDataReturnType, Hex } from 'viem'

const supabaseUrl = process.env.SUPABASE_URL
const supabaseKey = process.env.SUPABASE_KEY
const supabaseInstance = process.env.SUPABASE_INSTANCE

if (!supabaseUrl || !supabaseKey) {
    throw new Error('Supabase URL and Key must be provided in .env file')
}

// Create a single supabase client for interacting with your database
const supabase = createClient<Database>(supabaseUrl, supabaseKey)

const TOKEN_COOKIE_0 = `sb-${supabaseInstance}-auth-token.0`
const TOKEN_COOKIE_1 = `sb-${supabaseInstance}-auth-token.1`

const app = express()
const port = 3001

app.use(express.json())
app.use(express.urlencoded({ extended: true }))
app.use(cookieParser())

/**
 * Check if the request is authenticated
 */

type Session = {
    access_token: string
    refresh_token: string
}

app.use(async (req, res, next) => {
    const authCookiePart0 = req.cookies[TOKEN_COOKIE_0]
    const authCookiePart1 = req.cookies[TOKEN_COOKIE_1]
    const authCookie = authCookiePart0 + authCookiePart1

    if (typeof authCookie !== 'string' || !authCookie.startsWith('base64-')) {
        res.status(401).send('Unauthorized')
        return
    }

    // If it's a base64 encoded string, decode it
    const base64Str = authCookie.replace('base64-', '')
    const jsonStr = Buffer.from(base64Str, 'base64').toString()
    const session = JSON.parse(jsonStr) as Session

    const { data, error } = await supabase.auth.setSession(session)
    if (error) {
        console.error('Error setting session:', error)
        res.status(401).send('Unauthorized')
        return
    }
    if (!data.session) {
        console.log('No session found')
        res.status(401).send('Unauthorized')
        return
    }
    next()
})

app.get('/key', async (req, res) => {
    const { data, error } = await supabase
        .from('wallets')
        .select('private_key')
        .limit(1)
    if (!data) {
        console.error('Error fetching data:', error)
        res.status(500).send('Error fetching data')
        return
    }
    if (data.length === 0) {
        res.status(404).send('No keys found')
        return
    }
    const privateKey = data[0].private_key
    res.status(200).json({ privateKey })
})

type SignBody = {
    data: EncodeFunctionDataReturnType
    to: Hex
    value: string
}

app.post('/sign', async (req, res) => {
    const { data: txData } = req.body as SignBody
    if (!txData) {
        res.status(400).send('No data to sign')
        return
    }
    const viemService = useViemService()
    const { data, error } = await supabase
        .from('wallets')
        .select('private_key')
        .limit(1)
    if (!data) {
        console.error('Error fetching data:', error)
        res.status(500).send('Error fetching data')
        return
    }
    if (data.length === 0) {
        res.status(404).send('No keys found')
        return
    }
    const privateKey = data[0].private_key as Hex
    const account = viemService.getAccount(privateKey)

    const value = req.body.value ? BigInt(req.body.value) : undefined
    const to = req.body.to as Hex

    console.log('Preparing transaction request with data...')
    const request = await viemService.client.prepareTransactionRequest({
        data: txData,
        to,
        value,
        account,
    })
    console.log('Signing transaction with account:', account.address)
    const signature = await viemService.client.signTransaction(request)
    console.log('Transaction signed successfully')
    res.status(200).json({ signature })
})

app.listen(port, () => {
    console.log(`[Crypto estate - key management] listening on port ${port}`)
})
