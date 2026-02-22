const TABS = [
  { id: 'onoff', label: 'On/Off Impact' },
  { id: 'lineups', label: 'Lineup Data' },
  { id: 'teams', label: 'Team Ratings' },
  { id: 'gamelogs', label: 'Game Logs' },
] as const;

export type TabId = (typeof TABS)[number]['id'];

interface MainTabsProps {
  activeTab: TabId;
  onTabChange: (tab: TabId) => void;
}

export default function MainTabs({ activeTab, onTabChange }: MainTabsProps) {
  return (
    <nav className="tab-nav">
      {TABS.map(tab => (
        <button
          key={tab.id}
          className={`tab-btn ${activeTab === tab.id ? 'active' : ''}`}
          onClick={() => onTabChange(tab.id)}
        >
          {tab.label}
        </button>
      ))}
    </nav>
  );
}
