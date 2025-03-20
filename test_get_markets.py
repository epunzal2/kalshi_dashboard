import os
import json
from dotenv import load_dotenv
import unittest
from cryptography.hazmat.primitives import serialization
from src.clients import KalshiHttpClient, Environment

load_dotenv()

class TestMarketAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Initialize API client with demo credentials"""
        cls.env = Environment.DEMO
        cls.key_id = os.environ.get('DEMO_KEYID')
        keyfile_path = os.environ.get('DEMO_KEYFILE')
        
        if not cls.key_id or not keyfile_path:
            raise unittest.SkipTest("DEMO credentials not configured")

        with open(keyfile_path, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None
            )
            
        cls.client = KalshiHttpClient(
            key_id=cls.key_id,
            private_key=private_key,
            environment=cls.env
        )

    def test_single_series_ticker(self):
        """Test markets endpoint with single series ticker (KXCPIYOY)"""
        response = self.client.get_markets(series_ticker="KXCPIYOY")
        
        # Basic response validation
        self.assertIsInstance(response, list)
        self.assertGreater(len(response), 0)
        for market in response:
            self.assertIn('ticker', market)
        
        # Save results
        output_dir = "test_outputs"
        os.makedirs(output_dir, exist_ok=True)
        with open(f"{output_dir}/KXCPIYOY.json", "w") as f:
            json.dump(response, f, indent=2)

    def test_multiple_series_tickers(self):
        """Test markets endpoint with multiple series tickers"""
        response = self.client.get_markets(
            series_ticker="KXCPIYOY,KXNETFLIXRANKSHOW"
        )
        
        self.assertIsInstance(response, list)
        self.assertGreater(len(response), 0)
        
        # Verify at least one market from each series exists
        tickers = [m['ticker'] for m in response]
        self.assertTrue(any("KXCPIYOY" in t for t in tickers))
        self.assertTrue(any("KXNETFLIXRANKSHOW" in t for t in tickers))
        
        with open("test_outputs/KXCPIYOY_KXNETFLIXRANKSHOW.json", "w") as f:
            json.dump(response, f, indent=2)

    def test_event_ticker(self):
        """Test markets endpoint with event ticker (KXCPIYOY-25MAR)"""
        response = self.client.get_markets(event_ticker="KXCPIYOY-25MAR")
        
        self.assertIsInstance(response, list)
        self.assertGreater(len(response), 0)
        
        # All markets should belong to the specified event
        for market in response:
            self.assertIn("KXCPIYOY-25MAR", market['ticker'])
        
        with open("test_outputs/KXCPIYOY-25MAR.json", "w") as f:
            json.dump(response, f, indent=2)

class TestProdMarketAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Initialize API client with prod credentials"""
        cls.env = Environment.PROD
        cls.key_id = os.environ.get('PROD_KEYID')
        keyfile_path = os.environ.get('PROD_KEYFILE')
        
        if not cls.key_id or not keyfile_path:
            raise unittest.SkipTest("PROD credentials not configured")

        with open(keyfile_path, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None
            )
            
        cls.client = KalshiHttpClient(
            key_id=cls.key_id,
            private_key=private_key,
            environment=cls.env
        )

    def test_single_series_ticker(self):
        """PROD: Test markets endpoint with single series ticker"""
        response = self.client.get_markets(series_ticker="KXCPIYOY")
        self._validate_and_save(response, "PROD_KXCPIYOY.json")

    def test_specific_market_ticker(self):
        """PROD: Test markets endpoint with specific market ticker"""
        response = self.client.get_markets(
            tickers="KXCPIYOY-25MAR-T2.5"
        )
        self._validate_and_save(response, "PROD_KXCPIYOY-25MAR-T2.5.json")

    def test_event_ticker(self):
        """PROD: Test markets endpoint with event ticker"""
        response = self.client.get_markets(event_ticker="KXCPIYOY-25MAR")
        self._validate_and_save(response, "PROD_KXCPIYOY-25MAR.json")

    def _validate_and_save(self, response, filename):
        self.assertIsInstance(response, list)
        self.assertGreater(len(response), 0)
        
        output_dir = os.path.join("test_outputs", "prod")
        os.makedirs(output_dir, exist_ok=True)
        
        with open(os.path.join(output_dir, filename), "w") as f:
            json.dump(response, f, indent=2)

if __name__ == "__main__":
    unittest.main()
