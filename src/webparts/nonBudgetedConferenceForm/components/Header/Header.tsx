import * as React from 'react';
import {
    makeStyles,
    shorthands,
    tokens,
    TabList,
    Tab,
    Text,
    Avatar
} from '@fluentui/react-components';
import { useAppContext } from '../../../../context/AppContext';

const useStyles = makeStyles({
    root: {
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        backgroundColor: tokens.colorNeutralBackground1,
        ...shorthands.padding('16px', '24px'),
        ...shorthands.borderRadius('12px'),
        boxShadow: tokens.shadow4,
        border: `1px solid ${tokens.colorNeutralStroke1}`
    },
    leftBlock: {
        display: 'flex',
        flexDirection: 'column',
        gap: '2px',
    },
    title: {
        fontSize: '22px',
        fontWeight: '700',
        color: tokens.colorNeutralForeground1
    },
    subtitle: {
        fontSize: '13px',
        color: tokens.colorNeutralForeground3
    },
    navGroup: {
        display: 'flex',
        alignItems: 'center'
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
            <div className={styles.leftBlock}>
                <Text className={styles.title}>Conference & Event Form</Text>
                <Text className={styles.subtitle}>Non-Budgeted Request Portal</Text>
            </div>

            <div className={styles.navGroup}>
                <TabList
                    selectedValue={currentView}
                    onTabSelect={(e, data) => setCurrentView(data.value as any)}
                >
                    <Tab value="User">My Requests</Tab>
                    {roles.isManager && <Tab value="Manager">Manager Queue</Tab>}
                    {roles.isOrgDev && <Tab value="OrgDev">Org Dev Review</Tab>}
                    {roles.isAccounting && <Tab value="Accounting">Accounting Review</Tab>}
                </TabList>
            </div>

            <div className={styles.userGroup}>
                <Avatar name={currentUser.displayName} color="brand" />
                <Text weight="semibold" size={300}>{currentUser.displayName}</Text>
            </div>
        </div>
    );
};
