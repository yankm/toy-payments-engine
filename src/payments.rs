use crate::error::TransactionError;

use rust_decimal::Decimal;

pub type AccountId = u16;
pub type TransactionId = u32;

/// Funds in a customer account. `total` is the total amount of money in account (including held funds)
/// and `held` is the amount of money held due to disputes.
#[derive(Debug, Default)]
struct Funds {
    total: Decimal,
    held: Decimal,
}

impl Funds {
    /// Returns funds currently available for withdrawal.
    pub fn available(&self) -> Decimal {
        self.total - self.held
    }
}

/// A customer account holding client funds.
#[derive(Debug)]
pub struct Account {
    id: AccountId,
    funds: Funds,
}

impl Account {
    pub fn new(id: AccountId) -> Self {
        Self {
            id,
            funds: Funds::default(),
        }
    }

    #[cfg(test)]
    fn new_with_funds(id: AccountId, funds: Funds) -> Self {
        Self { id, funds }
    }

    pub fn id(&self) -> AccountId {
        self.id
    }

    /// Deposit funds to the account.
    pub fn deposit_funds(&mut self, amount: Decimal) -> Result<(), TransactionError> {
        if amount <= Decimal::ZERO {
            return Err(TransactionError::NonPositiveAmount);
        }
        self.funds.total = self
            .funds
            .total
            .checked_add(amount)
            .ok_or(TransactionError::TooMuchMoney)?;
        Ok(())
    }

    /// Withdraw funds from the account.
    pub fn withdraw_funds(&mut self, amount: Decimal) -> Result<(), TransactionError> {
        if amount <= Decimal::ZERO {
            return Err(TransactionError::NonPositiveAmount);
        }
        if amount > self.funds.available() {
            return Err(TransactionError::InsufficientFunds);
        }
        self.funds.total -= amount;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::TransactionError::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_account_add_funds_success() -> Result<(), TransactionError> {
        let mut acc = Account::new(0);
        acc.deposit_funds(dec!(1.23))?;
        assert_eq!(acc.funds.total, dec!(1.23));
        acc.deposit_funds(dec!(1.23))?;
        assert_eq!(acc.funds.total, dec!(2.46));
        Ok(())
    }

    #[test]
    fn test_account_add_non_positive_funds() {
        let amounts = [dec!(-1), dec!(-0.1), dec!(0)];
        let mut acc = Account::new(0);
        for amount in amounts {
            let result = acc.deposit_funds(amount);
            let expected = Err(NonPositiveAmount);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_account_add_funds_overflow() -> Result<(), TransactionError> {
        let mut acc = Account::new(0);
        let expected_funds = Decimal::MAX - dec!(10);
        acc.deposit_funds(expected_funds)?;

        let expected_error = Err(TooMuchMoney);
        let result = acc.deposit_funds(dec!(15));
        assert_eq!(result, expected_error);
        assert_eq!(acc.funds.total, expected_funds);
        Ok(())
    }

    #[test]
    fn test_account_withdraw_funds_success() -> Result<(), TransactionError> {
        let tests = [
            // funds_total, funds_held, withdraw_amount, expected_total
            (dec!(1.23), dec!(0), dec!(1.23), dec!(0)),
            (dec!(1.23), dec!(1.22), dec!(0.01), dec!(1.22)),
            (dec!(10), dec!(1), dec!(5), dec!(5)),
        ];
        for (total, held, withdraw_amount, expected_total) in tests {
            let mut acc = Account::new_with_funds(0, Funds { total, held });
            acc.withdraw_funds(withdraw_amount)?;
            assert_eq!(acc.funds.total, expected_total);
        }
        Ok(())
    }

    #[test]
    fn test_account_withdraw_non_positive_funds() {
        let amounts = [dec!(-1), dec!(-0.1), dec!(0)];

        let mut acc = Account::new_with_funds(
            0,
            Funds {
                total: dec!(2.46),
                held: dec!(0),
            },
        );
        for amount in amounts {
            let result = acc.withdraw_funds(amount);
            let expected = Err(NonPositiveAmount);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_account_withdraw_more_than_available_funds() {
        let tests = [
            // funds_total, funds_held, withdraw_amount
            (dec!(0), dec!(0), dec!(0.1)),
            (dec!(10), dec!(10), dec!(1)),
            (dec!(5), dec!(3), dec!(3)),
        ];

        for (total, held, withdraw_amount) in tests {
            let mut acc = Account::new_with_funds(0, Funds { total, held });
            let result = acc.withdraw_funds(withdraw_amount);
            assert_eq!(result, Err(InsufficientFunds));
        }
    }
}
