def get_sftp():
    print('sftp 작업 테스트입니다')

def regist(name, *args):
    print(f'이름: {name}')
    print(f'기타옵션:{args}')


def regist2(name, *args, **kwargs):
    print(f'이름: {name}')
    print(f'옵션:{args}')
    email = kwargs['email'] or None
    phone = kwargs['phone'] or None
    if email:
        print(email)
    if phone:
        print(phone)
