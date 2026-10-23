import unittest
from pathlib import Path
from ais_pipeline.generator.fleet import load_config, build_routes, vessel_reports

CONFIG = Path(__file__).resolve().parents[1] / "config/fleet.yaml"

class GeneratorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = load_config(CONFIG)
        cls.routes = build_routes(CONFIG, cls.config)

    def test_seeded_routes_and_delivery(self):
        a = list(vessel_reports(7, self.config, self.routes))
        self.assertEqual(a, list(vessel_reports(7, self.config, self.routes)))
        self.assertNotEqual(a, list(vessel_reports(8, self.config, self.routes)))
        self.assertTrue(all('vessel_id' not in r and r['IMO'] is None for r in a))
        self.assertEqual(len(a), len({r['reception_id'] for r in a}))
        for r in a:
            nominal = (r['event_time_s']-self.config['start_epoch'])//self.config['release_seconds']
            self.assertGreaterEqual(r['arrival_batch'],nominal)
        self.assertTrue(any(r['arrival_batch'] > (r['event_time_s']-self.config['start_epoch'])//self.config['release_seconds'] for r in a))

    def test_endpoints_and_dwell(self):
        config = {**self.config, 'jitter_metres':0, 'lane_sigma_metres':0, 'dropout_percent':0, 'reports_per_vessel':600}
        rows = list(vessel_reports(0,config,self.routes))
        self.assertTrue(any(r['Status']==5 for r in rows))
        ports = {(p['lon'],p['lat']) for p in config['ports']}
        for r in rows:
            if r['Status']==5:
                self.assertIn((r['LON'],r['LAT']),ports)
                self.assertEqual(r['SOG'],0)
        self.assertTrue(any(r['source']=='receiver-2' for r in rows))

if __name__ == '__main__':
    unittest.main()
