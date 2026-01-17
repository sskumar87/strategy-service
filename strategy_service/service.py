"""
Trading Strategy Service
Generates trading signals and publishes order messages
"""
import asyncio
import logging
from typing import List, Optional
from datetime import datetime
import random

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.broker import MessageBroker, BrokerFactory
from core.schemas import (
    NewOrderMessage, OrderType, MessageType, TradingSignalMessage
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TradingStrategy:
    """Base class for trading strategies"""
    
    def __init__(self, name: str):
        self.name = name
    
    async def generate_signal(self) -> Optional[TradingSignalMessage]:
        """Generate trading signal - to be implemented by subclasses"""
        raise NotImplementedError


class SimpleMovingAverageCrossover(TradingStrategy):
    """Example: Simple moving average crossover strategy"""
    
    def __init__(self, symbol: str = "EURUSD"):
        super().__init__("SMA_Crossover")
        self.symbol = symbol
    
    async def generate_signal(self) -> Optional[TradingSignalMessage]:
        """
        In a real implementation, this would:
        1. Fetch market data
        2. Calculate indicators
        3. Generate signals based on strategy logic
        
        For this example, we'll simulate random signals
        """
        # Simulate signal generation
        await asyncio.sleep(random.uniform(5, 15))
        
        action = random.choice(["BUY", "SELL", "NONE"])
        
        if action == "NONE":
            return None
        
        # Generate signal
        signal = TradingSignalMessage(
            message_id="",
            message_type=MessageType.TRADING_SIGNAL,
            timestamp="",
            source_service="strategy_service",
            signal_id=f"signal_{datetime.now().timestamp()}",
            symbol=self.symbol,
            action=action,
            strength=random.uniform(0.6, 0.9),
            entry_price=None,  # Would be calculated from market data
            stop_loss=None,
            take_profit=None,
            metadata={
                "strategy": self.name,
                "timeframe": "M15"
            }
        )
        
        return signal


class StrategyService:
    """Service that runs trading strategies and publishes orders"""
    
    def __init__(self, broker: MessageBroker, strategies: List[TradingStrategy],
                 target_accounts: Optional[List[str]] = None):
        self.broker = broker
        self.strategies = strategies
        self.target_accounts = target_accounts  # If None, broadcast to all accounts
        self._running = False
    
    async def start(self):
        """Start the strategy service"""
        logger.info("Starting Strategy Service")
        
        # Connect to broker
        await self.broker.connect()
        
        self._running = True
        
        # Start strategy tasks
        tasks = [
            asyncio.create_task(self._run_strategy(strategy))
            for strategy in self.strategies
        ]
        
        logger.info(f"Strategy Service started with {len(self.strategies)} strategies")
        
        await asyncio.gather(*tasks)
    
    async def stop(self):
        """Stop the strategy service"""
        logger.info("Stopping Strategy Service")
        self._running = False
        await self.broker.disconnect()
        logger.info("Strategy Service stopped")
    
    async def _run_strategy(self, strategy: TradingStrategy):
        """Run a single strategy"""
        logger.info(f"Starting strategy: {strategy.name}")
        
        while self._running:
            try:
                # Generate signal
                signal = await strategy.generate_signal()
                
                if signal:
                    logger.info(f"Signal generated: {signal.action} {signal.symbol} "
                              f"(strength: {signal.strength:.2f})")
                    
                    # Publish signal
                    await self.broker.publish('signals/trading_signals', signal.to_json())
                    
                    # Convert signal to order and publish
                    order = self._signal_to_order(signal)
                    if order:
                        await self.publish_order(order)
            
            except Exception as e:
                logger.error(f"Error in strategy {strategy.name}: {e}", exc_info=True)
                await asyncio.sleep(5)
    
    def _signal_to_order(self, signal: TradingSignalMessage) -> Optional[NewOrderMessage]:
        """Convert trading signal to order message"""
        if signal.action not in ["BUY", "SELL"]:
            return None
        
        # Determine order type
        order_type = OrderType.BUY if signal.action == "BUY" else OrderType.SELL
        
        # Calculate position size (example: fixed 0.1 lots)
        volume = 0.1
        
        # Create order message
        order = NewOrderMessage(
            message_id="",
            message_type=MessageType.NEW_ORDER,
            timestamp="",
            source_service="strategy_service",
            symbol=signal.symbol,
            order_type=order_type,
            volume=volume,
            price=signal.entry_price,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            comment=f"Signal {signal.signal_id[:8]}",
            magic_number=234000,
            target_accounts=self.target_accounts
        )
        
        return order
    
    async def publish_order(self, order: NewOrderMessage):
        """Publish order to broker"""
        try:
            logger.info(f"Publishing order: {order.order_type.value} {order.volume} {order.symbol}")
            await self.broker.publish('orders/new_order', order.to_json())
        except Exception as e:
            logger.error(f"Failed to publish order: {e}", exc_info=True)
    
    async def manual_order(self, symbol: str, order_type: OrderType, volume: float,
                          stop_loss: Optional[float] = None, 
                          take_profit: Optional[float] = None,
                          target_accounts: Optional[List[str]] = None):
        """Manually create and publish an order"""
        order = NewOrderMessage(
            message_id="",
            message_type=MessageType.NEW_ORDER,
            timestamp="",
            source_service="strategy_service_manual",
            symbol=symbol,
            order_type=order_type,
            volume=volume,
            stop_loss=stop_loss,
            take_profit=take_profit,
            comment="Manual order",
            target_accounts=target_accounts or self.target_accounts
        )
        
        await self.publish_order(order)


# Example usage
async def main():
    """Example main function"""
    # Create broker
    broker = BrokerFactory.create_broker('redis', host='localhost', port=6379)
    
    # Create strategies
    strategies = [
        SimpleMovingAverageCrossover("EURUSD"),
        SimpleMovingAverageCrossover("GBPUSD"),
    ]
    
    # Create strategy service
    # target_accounts=None means broadcast to all accounts
    # Or specify: target_accounts=["account_1", "account_2"]
    service = StrategyService(
        broker=broker,
        strategies=strategies,
        target_accounts=None  # Broadcast to all accounts
    )
    
    try:
        # Start service
        await service.start()
    
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())
