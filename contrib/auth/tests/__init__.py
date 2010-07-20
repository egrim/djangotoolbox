from djangotoolbox.contrib.auth.tests.basic import BASIC_TESTS
from djangotoolbox.contrib.auth.tests.views \
        import  ChangePasswordTest, LoginTest, LogoutTest#, PasswordResetTest
#from djangotoolbox.contrib.auth.tests.forms import FORM_TESTS
from djangotoolbox.contrib.auth.tests.remote_user \
        import RemoteUserTest, RemoteUserNoCreateTest, RemoteUserCustomTest
from djangotoolbox.contrib.auth.tests.auth_backends import AnonymousUserBackendTest, NoAnonymousUserBackendTest #,RowlevelBackendTest, BackendTest
from djangotoolbox.contrib.auth.tests.tokens import TOKEN_GENERATOR_TESTS
from djangotoolbox.contrib.auth.tests.models import ProfileTestCase

from djangotoolbox.contrib.auth.tests.groups import GroupPermissionTest

# The password for the fixture data users is 'password'

__test__ = {
    'BASIC_TESTS': BASIC_TESTS,
    #'FORM_TESTS': FORM_TESTS,
    'TOKEN_GENERATOR_TESTS': TOKEN_GENERATOR_TESTS,
}
