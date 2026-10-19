import { NavLink } from 'react-router-dom';

const tabs = [
  { to: '/onoff', fullLabel: 'On/Off Impact', shortLabel: 'On/Off', index: '01' },
  { to: '/lineups', fullLabel: 'Lineup Data', shortLabel: 'Lineups', index: '02' },
  { to: '/teams', fullLabel: 'Team Ratings', shortLabel: 'Teams', index: '03' },
  { to: '/gamelogs', fullLabel: 'Game Logs', shortLabel: 'Logs', index: '04' },
];

export function MainTabs() {
  return (
    <nav className="tabs" aria-label="Main tabs">
      {tabs.map((tab) => (
        <NavLink
          key={tab.to}
          to={tab.to}
          title={tab.fullLabel}
          aria-label={tab.fullLabel}
          className={({ isActive }) => `tab ${isActive ? 'active' : ''}`}
        >
          <span className="tab-index">{tab.index}</span>
          <span className="tab-label-short">{tab.shortLabel}</span>
        </NavLink>
      ))}
    </nav>
  );
}
