import { useState } from "react";
import { observer } from "mobx-react-lite";
import { Link, useNavigate } from "@tanstack/react-router";
import { useUser } from "../../hooks/store/use-user";

const SignUpPage = observer(() => {
  const userStore = useUser();
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [otp, setOtp] = useState("");
  const navigate = useNavigate();

  const handleSubmit = (e) => {
    e.preventDefault();
    userStore
      .createUser({
        provider: "email",
        fullname: name,
        email,
        password,
        provider_token: "string",
      })
      .then((resp) => {
        // userStore.isLoading = false;
        // navigate({ to: "/" });
      })
      .catch(() => {
        userStore.isLoading = false;
      });
  };

  const handleOtpSubmit = (e) => {
    e.preventDefault();

    userStore.verifyOtp({ otp }).then(() => {
      navigate({ to: "/signin" });
    });
  };
  return (
    <div>
      <h3>Sign Up</h3>
      <Link to="/signin">Sign in</Link>
      <form onSubmit={handleSubmit}>
        <fieldset>
          <div style={{ padding: 10, display: "flex", gap: 10 }}>
            <label htmlFor="name">
              Name
              <input
                id="name"
                type="text"
                value={name}
                onChange={(e) => setName(e.target.value)}
                required
              />
            </label>
          </div>
          <div style={{ padding: 10, display: "flex", gap: 10 }}>
            <label htmlFor="email">
              Email
              <input
                id="email"
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                required
              />
            </label>
          </div>
          <div style={{ padding: 10, display: "flex", gap: 10 }}>
            <label htmlFor="password">
              Password
              <input
                id="password"
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
              />
            </label>
          </div>
          <button type="submit">
            {userStore.isLoading ? "Loading..." : "Sign Up"}
          </button>
        </fieldset>
      </form>

      {userStore.showOtp && (
        <form onSubmit={handleOtpSubmit}>
          <div style={{ padding: 10, display: "flex", gap: 10 }}>
            <label htmlFor="otp">
              OTP
              <input
                id="otp"
                type="number"
                value={otp}
                onChange={(e) => setOtp(e.target.value)}
                required
              />
            </label>
          </div>
          <button type="submit">
            {userStore.isLoading ? "Loading..." : "verify"}
          </button>
        </form>
      )}
    </div>
  );
});

export default SignUpPage;