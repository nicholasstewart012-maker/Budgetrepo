import * as React from 'react';
import { useState, useMemo, useEffect } from 'react';
import { FluentProvider, webLightTheme, Spinner } from '@fluentui/react-components';
import { INonBudgetedConferenceFormProps } from './INonBudgetedConferenceFormProps';
import { AppContext, ViewType, IAppContext } from '../../../context/AppContext';
import { SharePointService } from '../../../services/SharePointService';
import { GraphService } from '../../../services/GraphService';
import { useCurrentUser } from '../../../hooks/useCurrentUser';
import { useManagerCheck } from '../../../hooks/useManagerCheck';
import { useRoleAccess } from '../../../hooks/useRoleAccess';
import { Header } from './Header/Header';
import { UserInterface } from './UserInterface/UserInterface';
import { ManagerInterface } from './ManagerInterface/ManagerInterface';
import { OrgDevInterface } from './OrgDevInterface/OrgDevInterface';
import { AccountingInterface } from './AccountingInterface/AccountingInterface';

export default function NonBudgetedConferenceForm(props: INonBudgetedConferenceFormProps) {
  const { context, listName, orgDevApprovers, accountingApprovers } = props;

  const spService = useMemo(() => new SharePointService(context, listName), [context, listName]);
  const graphService = useMemo(() => new GraphService(context), [context]);

  const currentUser = useCurrentUser(context);
  const { hasDirectReports, loading: graphLoading } = useManagerCheck(graphService);
  const { isOrgDev, isAccounting } = useRoleAccess(currentUser.email, orgDevApprovers, accountingApprovers);

  const [currentView, setCurrentView] = useState<ViewType>('User');

  useEffect(() => {
    if (isAccounting) {
      setCurrentView('Accounting');
    } else if (isOrgDev) {
      setCurrentView('OrgDev');
    } else {
      setCurrentView('User');
    }
  }, [isAccounting, isOrgDev]);

  const appContextValue: IAppContext = {
    context,
    currentUser,
    spService,
    graphService,
    roles: {
      isManager: hasDirectReports,
      isOrgDev,
      isAccounting
    },
    navigation: {
      currentView,
      setCurrentView
    },
    lists: {
      requestsList: listName
    }
  };

  const renderView = () => {
    switch (currentView) {
      case 'User':
        return <UserInterface />;
      case 'Manager':
        return hasDirectReports ? <ManagerInterface /> : null;
      case 'OrgDev':
        return isOrgDev ? <OrgDevInterface /> : null;
      case 'Accounting':
        return isAccounting ? <AccountingInterface /> : null;
      default:
        return <UserInterface />;
    }
  };

  return (
    <FluentProvider theme={webLightTheme}>
      <AppContext.Provider value={appContextValue}>
        {graphLoading ? (
          <Spinner label="Loading application..." />
        ) : (
          <div>
            <Header />
            {renderView()}
          </div>
        )}
      </AppContext.Provider>
    </FluentProvider>
  );
}
