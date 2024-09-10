access(all) struct AccountInfo {
    access(all) var address: Address
    access(all) var contracts: Int

    init(_ address: Address, _ contracts: Int) {
        self.address = address
        self.contracts = contracts
    }
}

access(all) fun main(addresses: [Address]): [AccountInfo] {
    let infos: [AccountInfo] = []
    for address in addresses {
        let account = getAccount(address)
        let contracts = account.contracts.names

        if contracts.length == 0 {
            continue
        }

        let info = AccountInfo(address, contracts.length)
        infos.append(info)
    }
    return infos
}
