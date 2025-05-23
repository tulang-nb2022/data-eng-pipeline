import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, List
import pytz

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TradingSchedule:
    """Manages trading schedule and request timing"""
    
    def __init__(self, requests_per_day: int = 25):
        self.ny_tz = pytz.timezone('America/New_York')
        # Core trading hours: 9:30 AM - 4:00 PM ET
        self.market_open = datetime.strptime('09:30', '%H:%M').time()
        self.market_close = datetime.strptime('16:00', '%H:%M').time()
        self.requests_per_day = requests_per_day
        self.request_times = self._calculate_request_times()
    
    def _calculate_request_times(self) -> List[datetime]:
        """Calculate evenly spaced request times during trading hours"""
        today = datetime.now(self.ny_tz).date()
        market_open_dt = datetime.combine(today, self.market_open)
        market_close_dt = datetime.combine(today, self.market_close)
        
        # Calculate total trading minutes
        trading_minutes = int((market_close_dt - market_open_dt).total_seconds() / 60)
        
        # Calculate minutes between requests
        minutes_between_requests = trading_minutes / (self.requests_per_day - 1)
        
        # Generate request times
        request_times = []
        for i in range(self.requests_per_day):
            request_time = market_open_dt + timedelta(minutes=i * minutes_between_requests)
            request_times.append(request_time)
        
        return request_times
    
    def is_market_open(self) -> bool:
        """Check if the market is currently open"""
        now = datetime.now(self.ny_tz)
        
        # Check if it's a weekday
        if now.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
            return False
            
        current_time = now.time()
        return self.market_open <= current_time <= self.market_close
    
    def get_next_request_time(self) -> Optional[datetime]:
        """Get the next scheduled request time"""
        now = datetime.now(self.ny_tz)
        
        # If market is closed, return next market open
        if not self.is_market_open():
            next_day = now + timedelta(days=1)
            next_day = next_day.replace(
                hour=self.market_open.hour,
                minute=self.market_open.minute,
                second=0,
                microsecond=0
            )
            return next_day
        
        # Find the next request time
        for request_time in self.request_times:
            if request_time > now:
                return request_time
        
        return None

class ErrorHandler:
    """Handles and logs errors for future alarm integration"""
    
    @staticmethod
    def handle_error(error: Exception, context: Dict = None):
        """Log error with context for future alarm integration"""
        error_context = {
            'timestamp': datetime.now().isoformat(),
            'error_type': type(error).__name__,
            'error_message': str(error),
            'context': context or {}
        }
        
        logger.error(
            f"Error occurred: {error_context['error_type']} - {error_context['error_message']}",
            extra=error_context
        )
        
        # Here you can add future alarm system integration
        # For example: send_to_alarm_system(error_context) 