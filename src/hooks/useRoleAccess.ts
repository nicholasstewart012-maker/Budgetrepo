import { useMemo } from 'react';
import { RoleService } from '../services/RoleService';

export const useRoleAccess = (userEmail: string, orgDevEmails: string, accountingEmails: string) => {
    const isOrgDev = useMemo(() => RoleService.isUserInRole(userEmail, orgDevEmails), [userEmail, orgDevEmails]);
    const isAccounting = useMemo(() => RoleService.isUserInRole(userEmail, accountingEmails), [userEmail, accountingEmails]);

    return { isOrgDev, isAccounting };
};
