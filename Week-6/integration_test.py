from time import sleep

from batch import main


def integration_test_for_prediction():
    ans = None
    while not ans:
        ans = main(2022, 1)
        sleep(1)

    assert ans == 31.51


if __name__ == '__main__':
    integration_test_for_prediction()
