pub struct AccountInfo {
    pub(set) var address: Address
    pub(set) var contracts: Int

    init(_ address: Address) {
        self.address = address
        self.contracts = 0
    }
}

pub fun main(addresses: [Address]): [AccountInfo] {
    let infos: [AccountInfo] = []
    for address in addresses {
        let account = getAccount(address)
        let contracts = account.contracts.names

        if contracts.length == 0 {
            continue
        }

        let info = AccountInfo(address)
        info.contracts = contracts.length
        infos.append(info)
    }
    return infos
}
