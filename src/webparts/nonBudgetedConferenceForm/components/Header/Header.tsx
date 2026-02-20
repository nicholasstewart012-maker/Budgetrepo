import * as React from 'react';
import {
    makeStyles,
    shorthands,
    tokens,
    Button,
    Title3,
    Avatar
} from '@fluentui/react-components';
import { useAppContext, ViewType } from '../../../../context/AppContext';

const useStyles = makeStyles({
    root: {
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        backgroundColor: tokens.colorNeutralBackground3,
        ...shorthands.padding('16px', '24px'),
        ...shorthands.borderRadius('8px'),
        marginBottom: '24px',
        boxShadow: tokens.shadow4,
    },
    navGroup: {
        display: 'flex',
        gap: '8px',
    },
    userGroup: {
        display: 'flex',
        alignItems: 'center',
        gap: '12px',
    }
});

export const Header: React.FC = () => {
    const styles = useStyles();
    const { currentUser, roles, navigation } = useAppContext();
    const { currentView, setCurrentView } = navigation;

    return (
        <div className={styles.root}>
            <Title3>Non-Budgeted Conference & Event Request</Title3>

            <div className={styles.navGroup}>
                <Button
                    appearance={currentView === 'User' ? 'primary' : 'subtle'}
                    onClick={() => setCurrentView('User')}
                >
                    My Requests / New Request
                </Button>

                {roles.isManager && (
                    <Button
                        appearance={currentView === 'Manager' ? 'primary' : 'subtle'}
                        onClick={() => setCurrentView('Manager')}
                    >
                        Manager Queue
                    </Button>
                )}

                {roles.isOrgDev && (
                    <Button
                        appearance={currentView === 'OrgDev' ? 'primary' : 'subtle'}
                        onClick={() => setCurrentView('OrgDev')}
                    >
                        Org Dev Review
                    </Button>
                )}

                {roles.isAccounting && (
                    <Button
                        appearance={currentView === 'Accounting' ? 'primary' : 'subtle'}
                        onClick={() => setCurrentView('Accounting')}
                    >
                        Accounting Review
                    </Button>
                )}
            </div>

            <div className={styles.userGroup}>
                <Avatar name={currentUser.displayName} />
            </div>
        </div>
    );
};
