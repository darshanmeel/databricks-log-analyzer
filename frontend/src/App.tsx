import { Link, Navigate, Route, Routes, useLocation, useParams } from 'react-router-dom';
import { ClusterLayout, Shell } from './components/Shell';
import Home from './pages/Home';
import Overview from './pages/Overview';
import Story from './pages/Story';
import HierarchyPage from './pages/Hierarchy';
import Timeline from './pages/Timeline';
import Executors from './pages/Executors';
import Logs from './pages/Logs';
import Findings from './pages/Findings';
import Errors from './pages/Errors';
import DataDebug from './pages/DataDebug';

/** Old Stages / Queries URLs open the same thing on the Queries & jobs page. */
function ToQueriesAndJobs() {
  const { cid, ctx, id } = useParams();
  const loc = useLocation();
  const p = new URLSearchParams(loc.search);
  if (ctx && id) {
    p.set('ctx', ctx);
    p.set('unit', `query:${ctx}:${id}`);
    if (!p.get('tab')) p.set('tab', 'plan');
  }
  const qs = p.toString();
  return <Navigate replace to={`/c/${encodeURIComponent(cid ?? '')}/hierarchy${qs ? `?${qs}` : ''}`} />;
}

function NotFound() {
  return (
    <div className="page">
      <div className="state">
        <h3>This page does not exist</h3>
        <p>
          <Link to="/">Go to the list of analyzed clusters</Link>
        </p>
      </div>
    </div>
  );
}

export default function App() {
  return (
    <Routes>
      <Route element={<Shell />}>
        <Route path="/" element={<div className="body no-nav"><main className="main"><Home /></main></div>} />
        <Route path="/c/:cid" element={<ClusterLayout />}>
          <Route index element={<Overview />} />
          <Route path="story" element={<Story />} />
          <Route path="hierarchy" element={<HierarchyPage />} />
          <Route path="timeline" element={<Timeline />} />
          <Route path="stages" element={<ToQueriesAndJobs />} />
          <Route path="queries" element={<ToQueriesAndJobs />} />
          <Route path="queries/:ctx/:id" element={<ToQueriesAndJobs />} />
          <Route path="executors" element={<Executors />} />
          <Route path="logs" element={<Logs />} />
          <Route path="findings" element={<Findings />} />
          <Route path="errors" element={<Errors />} />
          <Route path="data" element={<DataDebug />} />
          <Route path="*" element={<NotFound />} />
        </Route>
        <Route path="*" element={<div className="body no-nav"><main className="main"><NotFound /></main></div>} />
      </Route>
    </Routes>
  );
}
