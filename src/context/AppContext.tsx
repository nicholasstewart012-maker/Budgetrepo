import * as React from 'react';
import { WebPartContext } from '@microsoft/sp-webpart-base';
import { IUser } from '../models/IUser';
import { SharePointService } from '../services/SharePointService';
import { GraphService } from '../services/GraphService';

export type ViewType = 'User' | 'Manager' | 'OrgDev' | 'Accounting';

export interface IAppContext {
    context: WebPartContext;
    currentUser: IUser;
    spService: SharePointService;
    graphService: GraphService;
    roles: {
        isManager: boolean;
        isOrgDev: boolean;
        isAccounting: boolean;
    };
    navigation: {
        currentView: ViewType;
        setCurrentView: (view: ViewType) => void;
    };
    lists: {
        requestsList: string;
    };
}

export const AppContext = React.createContext<IAppContext | undefined>(undefined);

export const useAppContext = () => {
    const context = React.useContext(AppContext);
    if (!context) {
        throw new Error('useAppContext must be used within an AppProvider');
    }
    return context;
};
