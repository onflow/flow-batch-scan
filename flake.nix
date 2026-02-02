{
  description = "Flow Batch Scan - A library to scan the entire Flow blockchain";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        # Package license-eye (skywalking-eyes) since it's not in nixpkgs
        license-eye = pkgs.buildGoModule rec {
          pname = "license-eye";
          version = "0.8.0";

          src = pkgs.fetchFromGitHub {
            owner = "apache";
            repo = "skywalking-eyes";
            rev = "v${version}";
            hash = "sha256-uk/tX1AfYfy4ARzyd9IZijFYBEsfrx/DX+QsTVg3Jc4=";
          };

          vendorHash = "sha256-sH9zV99XX7f8q7guBDkLAJ7Lr3eiQXSQ2+CGd2zphLk=";

          subPackages = [ "cmd/license-eye" ];

          meta = with pkgs.lib; {
            description = "A full-featured license tool to check and fix license headers and dependencies' licenses";
            homepage = "https://github.com/apache/skywalking-eyes";
            license = licenses.asl20;
          };
        };
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            go_1_25
            golangci-lint
            just
            license-eye
          ];
        };
      });
}
