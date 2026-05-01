from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional
from decimal import Decimal
import uuid
import random


class TransactionType(Enum):
    """Enum for transaction types."""
    DEBIT = "DEBIT"
    CREDIT = "CREDIT"
    REVERSAL = "REVERSAL"


class TransactionStatus(Enum):
    """Enum for transaction status."""
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass
class Transaction:
    """
    Represents a financial transaction event.

    Attributes:
        transaction_id: Unique identifier for the transaction (UUID)
        fund_id: Identifier for the fund
        fund_name: Name of the fund
        deal_id: Identifier for the deal
        deal_name: Name of the deal
        transaction_amount: Amount of the transaction (as Decimal for precision)
        transaction_type: Type of transaction (DEBIT, CREDIT, REVERSAL)
        transaction_timestamp: When the transaction occurred (ISO 8601)
        currency: Currency code (ISO 4217, e.g., USD, EUR)
        status: Status of the transaction (PENDING, COMPLETED, FAILED)
        created_at: When the event was created in the system
    """
    transaction_id: str
    fund: dict
    deal: dict
    transaction_amount: Decimal
    transaction_type: TransactionType
    transaction_timestamp: datetime
    currency: str = "USD"
    status: TransactionStatus = TransactionStatus.COMPLETED
    created_at: Optional[datetime] = None

    def __post_init__(self):
        """Validate and set defaults."""
        if self.created_at is None:
            self.created_at = datetime.utcnow()

    def to_dict(self):
        """Convert transaction to dictionary for JSON serialization."""
        return {
            "transaction_id": self.transaction_id,
            "fund": self.fund,
            "deal": self.deal,
            # Decimal as string
            "transaction_amount": str(self.transaction_amount),
            "transaction_type": self.transaction_type.value,
            "transaction_timestamp": self.transaction_timestamp.isoformat(),
            "currency": self.currency,
            "status": self.status.value,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


funds = [
    {"fund_id": 1, "fund_name": "Fund A", "deals": [
        {"deal_id": 1, "deal_name": "Deal A"}, {"deal_id": 2, "deal_name": "Deal B"}]},
    {"fund_id": 2, "fund_name": "Fund B", "deals": [
        {"deal_id": 3, "deal_name": "Deal C"}]},
    {"fund_id": 3, "fund_name": "Fund C", "deals": [
        {"deal_id": 4, "deal_name": "Deal D"}]},
    {"fund_id": 4, "fund_name": "Fund D", "deals": [
        {"deal_id": 5, "deal_name": "Deal E"}, {"deal_id": 6, "deal_name": "Deal F"}]},
]


fund = random.choice(funds)
deal = random.choice(fund['deals'])

print(fund)
print(deal)


# # Example usage
# if __name__ == "__main__":
#     sample_transaction = Transaction(
#         transaction_id=str(uuid.uuid4()),
#         fund_id="FUND_001",
#         fund_name="Tech Growth Fund",
#         deal_id="DEAL_001",
#         deal_name="Series A Investment",
#         transaction_amount=Decimal("10000.00"),
#         transaction_type=TransactionType.CREDIT,
#         transaction_timestamp=datetime.utcnow()
#     )
#     print(sample_transaction.to_dict())
