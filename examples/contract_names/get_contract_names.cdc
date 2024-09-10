access(all) struct AccountInfo {
    access(all) var address: Address
    access(all) var contracts: [String]

    init(_ address: Address, _ contracts: [String]) {
        self.address = address
        self.contracts = contracts
    }
}

access(all) fun main(addresses: [Address]): [AccountInfo] {
    let infos: [AccountInfo] = []
    for address in addresses {
        let account = getAccount(address)
        let contracts: [String] = []

        for c in account.contracts.names {
            contracts.append(c)
        }

        if contracts.length == 0 {
            continue
        }

        let info = AccountInfo(address, contracts)
        infos.append(info)
    }
    return infos
}
