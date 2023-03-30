pub fun main(addresses: [Address]): Void {
    for address in addresses {
        let account = getAccount(address)
        // touch some registers
        account.storageUsed
    }

    return
}
